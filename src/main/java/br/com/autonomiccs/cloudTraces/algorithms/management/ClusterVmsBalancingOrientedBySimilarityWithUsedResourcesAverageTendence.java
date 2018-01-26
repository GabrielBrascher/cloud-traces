package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;

import br.com.autonomiccs.cloudTraces.beans.Host;
import br.com.autonomiccs.cloudTraces.beans.VirtualMachine;
import br.com.autonomiccs.cloudTraces.exceptions.GoogleTracesToCloudTracesException;

public class ClusterVmsBalancingOrientedBySimilarityWithUsedResourcesAverageTendence extends ClusterVmsBalancingOrientedBySimilarityWithUsedResources {

    private List<Host> originalManagedHostsList;
    //    private List<ComputingUsedResourcesHistoric> hostHistoricResourcesUsageList = new ArrayList<>();
    //    private List<ComputingUsedResourcesHistoric> vmsHistoricResourcesUsageList = new ArrayList<>();
    private Map<String, ComputingUsedResourcesHistoric> hostsIdAndHistoricMap;
    private Map<String, ComputingUsedResourcesHistoric> vmsIdAndHistoricMap;

    private StandardDeviation std = new StandardDeviation(false);

    private int numberOfVms = 0;
    protected double clusterCpuUsage, clusterMemoryUsage;
    private long totalClusterCpuInMhz = 0, usedClusterCpuInMhz = 0, totalClusterMemoryInMib = 0, usedClusterMemoryInMib = 0;
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
            long cpuUsedInMhz = 1;
            if (hostsIdAndHistoricMap.containsKey(h.getId())) {
                ComputingUsedResourcesHistoric hostHistoricResourcesUsage = hostsIdAndHistoricMap.get(h.getId());
                hostHistoricResourcesUsage.updateUsedResourceHistoricTendence(h.getCpuUsedInMhz(), h.getMemoryUsedInMib());
                cpuUsedInMhz = hostHistoricResourcesUsage.getHistoricTendenceMemoryUsedInMib();
            } else {
                ComputingUsedResourcesHistoric hostHistoricResourcesUsage = new ComputingUsedResourcesHistoric(h.getId());
                hostHistoricResourcesUsage.updateUsedResourceHistoricTendence(h.getCpuUsedInMhz(), h.getMemoryUsedInMib());
                cpuUsedInMhz = hostHistoricResourcesUsage.getHistoricTendenceMemoryUsedInMib();
                hostsIdAndHistoricMap.put(hostHistoricResourcesUsage.getId(), hostHistoricResourcesUsage);
            }

            long totalCpuInMhz = h.getTotalCpuPowerInMhz();
            double cpuUsagePercentage = ((double) cpuUsedInMhz / (double) totalCpuInMhz) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), cpuUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hostsSortedByMemoryUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsSortedByMemoryUsage) {
            //            ComputingUsedResourcesHistoric hostHistoricResourcesUsage = null;
            //            for (ComputingUsedResourcesHistoric hostHistoric : hostHistoricResourcesUsageList) {
            //                if (h.getId() == hostHistoric.getId()) {
            //                    hostHistoricResourcesUsage = hostHistoric;
            //                    break;
            //                }
            //            } TODO remover depois

            ComputingUsedResourcesHistoric hostHistoricResourcesUsage = hostsIdAndHistoricMap.get(h.getId());

            long totalMemoryInMib = h.getTotalMemoryInMib();
            long memoryUsedInMib = hostHistoricResourcesUsage.getHistoricTendenceMemoryUsedInMib();
            double memoryUsagePercentage = ((double) memoryUsedInMib / (double) totalMemoryInMib) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), memoryUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
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

    @Override
    protected long optimumUsedCpuInMhz(Host host) {
        long optimumUsageCpuInMhz = (long) (clusterCpuUsage * host.getTotalCpuPowerInMhz());
        return optimumUsageCpuInMhz;
    }

    @Override
    protected long optimumUsedMemoryInMib(Host host) {
        long optimumAllocatedMemoryInMib = (long) (clusterMemoryUsage * host.getTotalMemoryInMib());
        return optimumAllocatedMemoryInMib;
    }

    @Override
    protected void calculateClusterUsage(List<Host> rankedHosts) {
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
            if (vmsIdAndHistoricMap.containsKey(vm.getVmId())) {
                ComputingUsedResourcesHistoric vmHistoric = vmsIdAndHistoricMap.get(vm.getVmId());
                vmHistoric.updateUsedResourceHistoricTendence(vm.getCpuUsedInMhz(), vm.getMemoryUsedInMib());
                memoryValues.add(vmHistoric.getHistoricTendenceMemoryUsedInMib());
                cpuValuesInMhz.add(vmHistoric.getHistoricTendenceCpuUsedInMhz());
            } else {
                ComputingUsedResourcesHistoric vmHistoric = new ComputingUsedResourcesHistoric(vm.getVmId());
                vmHistoric.updateUsedResourceHistoricTendence(vm.getCpuUsedInMhz(), vm.getMemoryUsedInMib());
                memoryValues.add(vmHistoric.getHistoricTendenceMemoryUsedInMib());
                cpuValuesInMhz.add(vmHistoric.getHistoricTendenceCpuUsedInMhz());
                vmsIdAndHistoricMap.put(vm.getVmId(), vmHistoric);
            }
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

    /**
     * TODO
     */
    public class ComputingUsedResourcesHistoric {
        private String id;
        private long historicTendenceCpuUsedInMhz;
        private long historicTendenceMemoryUsedInMib;
        private List<Long> usedCpuInMhzList = new ArrayList<>();
        private List<Long> usedMemoryInMibList = new ArrayList<>();

        private static final int MAXIMUM_NUMBER_OF_HISTORIC_VALUES = 100;

        public ComputingUsedResourcesHistoric(String id) {
            this.id = id;
        }

        public void updateUsedResourceHistoricTendence(long cpuUsedInMhz, long memoryUsedInMib) {
            updateResourcesUsedHistoricValues(cpuUsedInMhz, memoryUsedInMib);

            List<Double> cpuValuesInMhz = new ArrayList<>();
            List<Double> memoryValuesInMib = new ArrayList<>();
            long sumOfCpuUsageInMhz = 0;
            long sumOfMemoryUsageInMib = 0;
            int numberOfValues = 0;

            for (int i = usedCpuInMhzList.size(); i > 0; i--) {
                numberOfValues++;
                sumOfCpuUsageInMhz += usedCpuInMhzList.get(i - 1);
                cpuValuesInMhz.add((double) sumOfCpuUsageInMhz / numberOfValues);

                sumOfMemoryUsageInMib += usedMemoryInMibList.get(i - 1);
                memoryValuesInMib.add((double) sumOfMemoryUsageInMib / numberOfValues);
            }

            long cpuInMhzWeightedMean = 0l;
            long memoryInMibWeightedMean = 0l;
            for (int j = 0; j < cpuValuesInMhz.size(); j++) {
                cpuInMhzWeightedMean += cpuValuesInMhz.get(j);
                memoryInMibWeightedMean += memoryValuesInMib.get(j);
            }

            cpuInMhzWeightedMean += historicTendenceCpuUsedInMhz;
            memoryInMibWeightedMean += historicTendenceMemoryUsedInMib;
            if (cpuValuesInMhz.size() > 1) {
                historicTendenceCpuUsedInMhz = cpuInMhzWeightedMean / (cpuValuesInMhz.size() + 1);
                historicTendenceMemoryUsedInMib = memoryInMibWeightedMean / (cpuValuesInMhz.size() + 1);
            } else {
                historicTendenceCpuUsedInMhz = cpuInMhzWeightedMean;
                historicTendenceMemoryUsedInMib = memoryInMibWeightedMean;
            }
        }

        /**
         * It removes the oldest CPU and Memory usage in the sample of resources usage and includes
         * new usage metrics.
         */
        private void updateResourcesUsedHistoricValues(long cpuUsedInMhz, long memoryUsedInMib) {
            if (usedCpuInMhzList.size() < MAXIMUM_NUMBER_OF_HISTORIC_VALUES) {
                usedCpuInMhzList.add(cpuUsedInMhz);
                usedMemoryInMibList.add(memoryUsedInMib);
            } else {
                usedCpuInMhzList.remove(0);
                usedCpuInMhzList.add(cpuUsedInMhz);

                usedMemoryInMibList.remove(0);
                usedMemoryInMibList.add(memoryUsedInMib);
            }
        }

        public String getId() {
            return id;
        }

        public long getHistoricTendenceCpuUsedInMhz() {
            return historicTendenceCpuUsedInMhz;
        }

        public long getHistoricTendenceMemoryUsedInMib() {
            return historicTendenceMemoryUsedInMib;
        }

        public List<Long> getCpuUsageInMhzSample() {
            return usedCpuInMhzList;
        }

        public List<Long> getMemoryUsageInMibSample() {
            return usedMemoryInMibList;
        }
    }

}
