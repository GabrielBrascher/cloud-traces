package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;

import br.com.autonomiccs.cloudTraces.beans.Host;
import br.com.autonomiccs.cloudTraces.beans.VirtualMachine;
import br.com.autonomiccs.cloudTraces.exceptions.GoogleTracesToCloudTracesException;

public class ClusterVmsBalancingOrientedBySimilarityWithUsedResources extends ClusterVmsBalancingOrientedBySimilarity {

    private List<Host> originalManagedHostsList;

    private int numberOfVms = 0;

    private StandardDeviation std = new StandardDeviation(false);

    protected double clusterCpuUsage, clusterMemoryUsage;
    long totalClusterCpuInMhz = 0, usedClusterCpuInMhz = 0, totalClusterMemoryInMib = 0, usedClusterMemoryInMib = 0;
    protected long vmsUsedCpuInMhzMedian, vmsUsedMemoryInMibMedian, vmsUsedCpuInMhzMean, vmsUsedMemoryInMibMean;

    private final static Logger logger = Logger.getLogger(ClusterVmsBalancingOrientedBySimilarity.class);

    protected static final long NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE = 1024l;
    protected static final String COSINE_SIMILARITY_WITH_HOST_VM_RATIO = "cosineSimilarityWithHostVmRatio";
    protected static final String COSINE_SIMILARITY = "cosineSimilarity";
    protected static final String COSIZE = "cosize";

    @Override
    public List<Host> rankHosts(List<Host> hosts) {
        originalManagedHostsList = hosts;

        List<Host> hostsToBeRanked = new ArrayList<>(hosts);
        List<ComputingResourceIdAndScore> hostsScoreByCpu = sortHostsByCpuUsage(hostsToBeRanked);
        List<ComputingResourceIdAndScore> hostsScoreByMemory = sortHostsByMemoryUsage(hostsToBeRanked);
        List<ComputingResourceIdAndScore> hostsSortedByUsage = calculateHostsScore(hostsScoreByCpu, hostsScoreByMemory);

        return sortHostUpwardScoreBasedOnSortedListOfComputingResourceIdAndScore(hostsSortedByUsage, hostsToBeRanked);
    }

    @Override
    public Map<VirtualMachine, Host> mapVMsToHost(List<Host> rankedHosts) {
        calculateClusterStatistics(rankedHosts);
        List<Double> standardDeviations = new ArrayList<>();
        List<Map<VirtualMachine, Host>> maps = new ArrayList<>();

        List<Host> rankedHostsCosineSimilarityMean = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsCosineSimilarityMedian = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsCosineSimilarityMean, 1, COSINE_SIMILARITY, vmsUsedCpuInMhzMean, vmsUsedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsCosineSimilarityMedian, 1, COSINE_SIMILARITY, vmsUsedCpuInMhzMedian, vmsUsedMemoryInMibMedian));
        standardDeviations.add(calculateStandarDeviation(rankedHostsCosineSimilarityMean));
        standardDeviations.add(calculateStandarDeviation(rankedHostsCosineSimilarityMedian));

        List<Host> rankedHostsMeanAlpha1 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMeanAlpha2 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMeanAlpha10 = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha1, 1, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMean, vmsUsedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha2, 2, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMean, vmsUsedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha10, 10, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMean, vmsUsedMemoryInMibMean));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha1));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha2));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha10));

        List<Host> rankedHostsMedianAlpha1 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMedianAlpha2 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMedianAlpha10 = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha1, 1, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMedian, vmsUsedMemoryInMibMedian));
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha2, 2, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMedian, vmsUsedMemoryInMibMedian));
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha10, 10, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsUsedCpuInMhzMedian, vmsUsedMemoryInMibMedian));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha1));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha2));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha10));

        return maps.get(standardDeviations.indexOf(Collections.min(standardDeviations)));
    }

    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByCpuUsage(List<Host> hostsToSortByCpuUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsToSortByCpuUsage) {
            double cpuUsagePercentage = ((double) h.getCpuUsedInMhz() / (double) h.getTotalCpuPowerInMhz()) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), cpuUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hostsSortedByMemoryUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsSortedByMemoryUsage) {
            double memoryUsagePercentage = ((double) h.getMemoryUsedInMib() / (double) h.getTotalMemoryInMib()) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), memoryUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected boolean isOverloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long maximumAcceptedCpuUsageInMhz = optimumUsedCpuInMhz(host) + vmsCpuProfile;
        long maximumAcceptedMemoryUsageInMib = optimumUsedMemoryInMib(host) + vmsMemoryProfile;

        if (host.getCpuUsedInMhz() >= maximumAcceptedCpuUsageInMhz) {
            return true;
        }
        if (host.getMemoryUsedInMib() >= maximumAcceptedMemoryUsageInMib) {
            return true;
        }
        return false;
    }

    @Override
    protected boolean isUnderloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long minimumAcceptedCpuUsageInMhz = optimumUsedCpuInMhz(host) - vmsCpuProfile;
        long minimumAcceptedMemoryUsageInMib = optimumUsedMemoryInMib(host) - vmsMemoryProfile;

        if (host.getCpuUsedInMhz() < minimumAcceptedCpuUsageInMhz) {
            return true;
        }
        if (host.getMemoryUsedInMib() < minimumAcceptedMemoryUsageInMib) {
            return true;
        }
        return false;
    }

    protected long optimumUsedCpuInMhz(Host host) {
        return (long) (clusterCpuUsage * host.getTotalCpuPowerInMhz());
    }

    protected long optimumUsedMemoryInMib(Host host) {
        return (long) (clusterMemoryUsage * host.getTotalMemoryInMib());
    }

    //TODO Used
    @Override
    protected double calculateStandarDeviation(List<Host> hosts) {
        double memoryUsage[] = new double[hosts.size()];
        double cpuUsage[] = new double[hosts.size()];

        for (int i = 0; i < hosts.size(); i++) {
            memoryUsage[i] = (double) hosts.get(i).getMemoryUsedInMib() / (double) hosts.get(i).getTotalMemoryInMib();
            cpuUsage[i] = (double) hosts.get(i).getCpuUsedInMhz() / (double) hosts.get(i).getTotalCpuPowerInMhz();
        }
        double memoryStd = std.evaluate(memoryUsage);
        double cpuStd = std.evaluate(cpuUsage);
        if (memoryStd > cpuStd) {
            return memoryStd;
        } else {
            return cpuStd;
        }
    }

    @Override
    protected void calculateClusterStatistics(List<Host> rankedHosts) {
        numberOfVms = 0;
        Set<VirtualMachine> vms = new HashSet<>();
        for (Host h : rankedHosts) {
            numberOfVms += h.getVirtualMachines().size();
            vms.addAll(h.getVirtualMachines());
        }
        if (numberOfVms == 0) {
            return;
        }
        calculateVmsCpuAndMemoryMedian(vms);
        calculateClusterUsage(rankedHosts);

        vmsUsedCpuInMhzMean = usedClusterCpuInMhz / numberOfVms;
        vmsUsedMemoryInMibMean = usedClusterMemoryInMib / numberOfVms;
    }

    @Override
    protected void calculateClusterUsage(List<Host> rankedHosts) {
        totalClusterCpuInMhz = 0;
        usedClusterCpuInMhz = 0;
        totalClusterMemoryInMib = 0;
        usedClusterMemoryInMib = 0;
        for (Host h : rankedHosts) {
            totalClusterCpuInMhz += h.getTotalCpuPowerInMhz();
            usedClusterCpuInMhz += h.getCpuUsedInMhz();
            totalClusterMemoryInMib += h.getTotalMemoryInMib();
            usedClusterMemoryInMib += h.getMemoryUsedInMib();
        }
        clusterCpuUsage = (double) usedClusterCpuInMhz / totalClusterCpuInMhz;
        clusterMemoryUsage = (double) usedClusterMemoryInMib / totalClusterMemoryInMib;
    }

    @Override
    protected Map<VirtualMachine, Host> simulateMapOfMigrations(List<Host> rankedHosts, double alpha, String similarityMethod, long vmsCpuInMhzStatistic,
            long vmsMemoryInMibStatistic) {
        Map<VirtualMachine, Host> mapOfMigrations = new HashMap<>();
        for (int i = rankedHosts.size() - 1; i > 0; i--) {
            Host hostToBeOffloaded = rankedHosts.get(i);
            if (isOverloaded(hostToBeOffloaded, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic)) {
                mapMigrationsFromHostToBeOffloaded(mapOfMigrations, hostToBeOffloaded, rankedHosts, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic, alpha, similarityMethod);
            }
        }
        return mapOfMigrations;
    }

    @Override
    protected void mapMigrationsFromHostToBeOffloaded(Map<VirtualMachine, Host> mapOfMigrations, Host hostToBeOffloaded, List<Host> rankedHosts, long vmsCpuInMhzStatistic,
            long vmsMemoryInMibStatistic, double alpha, String similarityMethod) {
        for (Host candidateToReceiveVms : rankedHosts) {
            if (!candidateToReceiveVms.equals(hostToBeOffloaded)) {
                if (isUnderloaded(candidateToReceiveVms, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic)) {
                    List<VirtualMachine> vms = sortVmsInDescendingOrderBySimilarityWithHostCandidateToReceiveVms(hostToBeOffloaded, candidateToReceiveVms, alpha, similarityMethod);
                    for (VirtualMachine vm : vms) {
                        if (vm.getHost().getId().equals(candidateToReceiveVms.getId())) {
                            continue;
                        }
                        if (isOverloaded(hostToBeOffloaded, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic)) {
                            try {
                                if (canReceiveVm(candidateToReceiveVms, vm, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic)) {
                                    Host sourceHost = vm.getHost();
                                    updateHostResources(hostToBeOffloaded, candidateToReceiveVms, vm);
                                    Host targetHost = findHostFromOriginalRankedList(candidateToReceiveVms);
                                    vm.setHost(findHostFromOriginalRankedList(sourceHost));

                                    mapOfMigrations.put(vm, targetHost);
                                }
                            } catch (CloneNotSupportedException e) {
                                logger.error("Problems while clonning objects", e);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected List<VirtualMachine> sortVmsInDescendingOrderBySimilarityWithHostCandidateToReceiveVms(Host hostToBeOffloaded, Host candidateToReceiveVms, double alpha,
            String similarityMethod) {
        List<VirtualMachine> vmsInHostToBeOffloaded = new ArrayList<>(hostToBeOffloaded.getVirtualMachines());
        List<VirtualMachine> sortedVirtualMachines = new ArrayList<>();

        List<ComputingResourceIdAndScore> sortedVmsIdAndScore = calculateSimilarity(candidateToReceiveVms, vmsInHostToBeOffloaded, alpha, similarityMethod);
        if (!CollectionUtils.isEmpty(sortedVmsIdAndScore)) {
            sortComputingResourceDowardScore(sortedVmsIdAndScore);
        }

        for (ComputingResourceIdAndScore vmIdAndScore : sortedVmsIdAndScore) {
            for (VirtualMachine vm : vmsInHostToBeOffloaded) {
                if (vmIdAndScore.getId().equals(vm.getVmId())) {
                    sortedVirtualMachines.add(vm);
                }
            }
        }
        return sortedVirtualMachines;
    }

    @Override
    protected void calculateVmsCpuAndMemoryMedian(Set<VirtualMachine> vms) {
        if (vms.size() == 0) {
            vmsUsedCpuInMhzMedian = 0;
            vmsUsedMemoryInMibMedian = 0;
            return;
        }
        List<Long> cpuValuesInMhz = new ArrayList<>();
        List<Long> memoryValues = new ArrayList<>();
        int middle = vms.size() / 2;

        for (VirtualMachine vm : vms) {
            memoryValues.add(vm.getMemoryUsedInMib());
            cpuValuesInMhz.add(vm.getCpuUsedInMhz());
        }

        Collections.sort(cpuValuesInMhz);
        Collections.sort(memoryValues);

        if (vms.size() % 2 == 1) {
            vmsUsedCpuInMhzMedian = cpuValuesInMhz.get(middle);
            vmsUsedMemoryInMibMedian = memoryValues.get(middle);
        } else {
            vmsUsedCpuInMhzMedian = (cpuValuesInMhz.get(middle - 1) + cpuValuesInMhz.get(middle)) / 2;
            vmsUsedMemoryInMibMedian = (memoryValues.get(middle - 1) + memoryValues.get(middle)) / 2;
        }
    }

    public class VmUsedResources {
        private long usedCpuInMhz;
        private long usedMemoryInMegaByte;

        public VmUsedResources(long usedCpuInMhz, long usedMemoryInMegaByte) {
            this.usedCpuInMhz = usedCpuInMhz;
            this.usedMemoryInMegaByte = usedMemoryInMegaByte;
        }

        public long getUsedCpuInMhz() {
            return usedCpuInMhz;
        }

        public long getUsedMemoryInMegaByte() {
            return usedMemoryInMegaByte;
        }
    }

    private List<ComputingResourceIdAndScore> calculateSimilarity(Host candidateToReceiveVms, List<VirtualMachine> vms, double alpha, String similarityMethod) {
        List<ComputingResourceIdAndScore> scoredVms = new ArrayList<>();
        for (VirtualMachine vm : vms) {
            long vmUsedCpuInMhz = vm.getCpuUsedInMhz();
            long vmUsedMemoryInMegaByte = vm.getMemoryUsedInMib();
            long[] vmConfigurationsVector = { vmUsedCpuInMhz, vmUsedMemoryInMegaByte };

            long candidateToReceiveVmsAvailableCpuInMhz = candidateToReceiveVms.getTotalCpuPowerInMhz() - candidateToReceiveVms.getCpuUsedInMhz();
            long candidateToReceiveVmsAvailableMemoryInMegaByte = candidateToReceiveVms.getTotalMemoryInMib() - candidateToReceiveVms.getMemoryUsedInMib();
            long[] hostAvailableCapacityVector = { candidateToReceiveVmsAvailableCpuInMhz, candidateToReceiveVmsAvailableMemoryInMegaByte };

            if (similarityMethod.equals(COSINE_SIMILARITY)) {
                double vmScore = cosineSimilarity(vmConfigurationsVector, hostAvailableCapacityVector);
                scoredVms.add(new ComputingResourceIdAndScore(vm.getVmId(), vmScore));
            } else if (similarityMethod.equals(COSINE_SIMILARITY_WITH_HOST_VM_RATIO)) {
                double vmScore = calculateCosineSimilarityWithHostVmRatio(alpha, vmConfigurationsVector, hostAvailableCapacityVector);
                scoredVms.add(new ComputingResourceIdAndScore(vm.getVmId(), vmScore));
            } else if (similarityMethod.equals(COSIZE)) {
                double vmScore = calculateCosize(alpha, vmConfigurationsVector, hostAvailableCapacityVector);
                scoredVms.add(new ComputingResourceIdAndScore(vm.getVmId(), vmScore));
            }
        }
        return scoredVms;
    }

    @Override
    protected boolean canReceiveVm(Host candidateToReceiveVms, VirtualMachine vm, long vmsCpuInMhzStatistic, long vmsMemoryInMibStatistic) throws CloneNotSupportedException {
        Host sourceHost = vm.getHost();
        Host clonedHost = (Host) candidateToReceiveVms.clone();
        clonedHost.addVirtualMachine(vm);
        boolean canTargetHostReceiveVm = !isOverloaded(clonedHost, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic);

        vm.setHost(sourceHost);
        candidateToReceiveVms.getVirtualMachines().remove(vm);
        return canTargetHostReceiveVm;
    }

    @Override
    protected Host findHostFromOriginalRankedList(Host candidateToReceiveVms) {
        for (Host h : originalManagedHostsList) {
            if (h.getId().equals(candidateToReceiveVms.getId())) {
                return h;
            }
        }
        throw new GoogleTracesToCloudTracesException(String.format("Failed to find a host with id=[%s] at the original hosts list.", candidateToReceiveVms));
    }

}
