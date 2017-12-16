package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;

import br.com.autonomiccs.cloudTraces.beans.Host;
import br.com.autonomiccs.cloudTraces.beans.VirtualMachine;
import br.com.autonomiccs.cloudTraces.exceptions.GoogleTracesToCloudTracesException;

public class ClusterVmsBalancingOrientedBySimilarity extends ClusterAdministrationAlgorithmEmptyImpl {

    protected double clusterCpuUsage, clusterMemoryUsage;
    private long totalClusterCpuInMhz = 0, allocatedClusterCpuInMhz = 0, totalClusterMemoryInMib = 0, allocatedClusterMemoryInMib = 0;
    protected long vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean;

    private StandardDeviation std = new StandardDeviation(false);

    protected static final long NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE = 1024l;
    protected static final String COSINE_SIMILARITY_WITH_HOST_VM_RATIO = "cosineSimilarityWithHostVmRatio";
    protected static final String COSINE_SIMILARITY = "cosineSimilarity";
    protected static final String COSIZE = "cosize";
    private static final boolean LOG_HEURISTICS_WITH_MINIMUM_STANDARD_DEVIATIONS = false;

    private List<Host> originalManagedHostsList;
    private int numberOfVms = 0;

    private final static Logger logger = Logger.getLogger(ClusterVmsBalancingOrientedBySimilarity.class);

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
        Map<VirtualMachine, Host> map = new HashMap<>();

        List<Host> rankedHostsCosineSimilarityMean = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsCosineSimilarityMedian = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsCosineSimilarityMean, 1, COSINE_SIMILARITY, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsCosineSimilarityMedian, 1, COSINE_SIMILARITY, vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian));
        standardDeviations.add(calculateStandarDeviation(rankedHostsCosineSimilarityMean));
        standardDeviations.add(calculateStandarDeviation(rankedHostsCosineSimilarityMedian));

        List<Host> rankedHostsMeanAlpha1 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMeanAlpha2 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMeanAlpha10 = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha1, 1, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha2, 2, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean));
        maps.add(simulateMapOfMigrations(rankedHostsMeanAlpha10, 10, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha1));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha2));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMeanAlpha10));

        List<Host> rankedHostsMedianAlpha1 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMedianAlpha2 = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsMedianAlpha10 = cloneListOfHosts(rankedHosts);
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha1, 1, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian));
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha2, 2, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian));
        maps.add(simulateMapOfMigrations(rankedHostsMedianAlpha10, 10, COSINE_SIMILARITY_WITH_HOST_VM_RATIO, vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha1));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha2));
        standardDeviations.add(calculateStandarDeviation(rankedHostsMedianAlpha10));

        if (LOG_HEURISTICS_WITH_MINIMUM_STANDARD_DEVIATIONS) {
            int indexOfMinimumStandardDeviation = standardDeviations.indexOf(Collections.min(standardDeviations));
            String minimumStdResults = logHeuristicsWithMinimumStandardDeviations(standardDeviations, indexOfMinimumStandardDeviation);

            map = maps.get(indexOfMinimumStandardDeviation);
            if (map.size() > 0) {
                logger.info(minimumStdResults);
                logger.info(String.format("selected map index [%d], number of hosts [%s], number of VMs [%s]", indexOfMinimumStandardDeviation, rankedHosts.size(), numberOfVms));
            }
        } else {
            int indexOfMinimumStandardDeviation = standardDeviations.indexOf(Collections.min(standardDeviations));
            map = maps.get(indexOfMinimumStandardDeviation);
        }
        return map;
    }

    private String logHeuristicsWithMinimumStandardDeviations(List<Double> standardDeviations, int indexOfMinimumStandardDeviation) {
        String minimumStdResults = "MinimumResults=[";
        for (int i = 0; i < standardDeviations.size(); i++) {
            if (standardDeviations.get(i) <= standardDeviations.get(indexOfMinimumStandardDeviation)) {
                minimumStdResults += "1 ";
            } else {
                minimumStdResults += "0 ";
            }
        }
        minimumStdResults += "]";
        return minimumStdResults;
    }

    protected double calculateStandarDeviation(List<Host> hosts) {
        double memoryUsage[] = new double[hosts.size()];
        double cpuUsage[] = new double[hosts.size()];

        for (int i = 0; i < hosts.size(); i++) {
            memoryUsage[i] = (double) hosts.get(i).getMemoryAllocatedInMib() / (double) hosts.get(i).getTotalMemoryInMib();
            cpuUsage[i] = (double) hosts.get(i).getCpuAllocatedInMhz() / (double) hosts.get(i).getTotalCpuPowerInMhz();
        }
        double memoryStd = std.evaluate(memoryUsage);
        double cpuStd = std.evaluate(cpuUsage);
        if (memoryStd > cpuStd) {
            return memoryStd;
        } else {
            return cpuStd;
        }
    }

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

    private Host findHostFromOriginalRankedList(Host candidateToReceiveVms) {
        for (Host h : originalManagedHostsList) {
            if (h.getId().equals(candidateToReceiveVms.getId())) {
                return h;
            }
        }
        throw new GoogleTracesToCloudTracesException(String.format("Failed to find a host with id=[%s] at the original hosts list.", candidateToReceiveVms));
    }

    protected boolean canReceiveVm(Host candidateToReceiveVms, VirtualMachine vm, long vmsCpuInMhzStatistic, long vmsMemoryInMibStatistic) throws CloneNotSupportedException {
        Host sourceHost = vm.getHost();
        Host clonedHost = (Host) candidateToReceiveVms.clone();
        clonedHost.addVirtualMachine(vm);
        boolean canTargetHostReceiveVm = !isOverloaded(clonedHost, vmsCpuInMhzStatistic, vmsMemoryInMibStatistic);

        vm.setHost(sourceHost);
        candidateToReceiveVms.getVirtualMachines().remove(vm);
        return canTargetHostReceiveVm;
    }

    protected void updateHostResources(Host hostToBeOffloaded, Host candidateToReceiveVms, VirtualMachine vm) {
        long vmCpuConfigurationInMhz = vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores();
        long vmMemoryConfigurationInBytes = vm.getVmServiceOffering().getMemoryInMegaByte() * NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE;

        hostToBeOffloaded.getVirtualMachines().remove(vm);
        hostToBeOffloaded.setCpuAllocatedInMhz(hostToBeOffloaded.getCpuAllocatedInMhz() - vmCpuConfigurationInMhz);
        hostToBeOffloaded.setMemoryAllocatedInBytes(hostToBeOffloaded.getMemoryAllocatedInBytes() - vmMemoryConfigurationInBytes);

        candidateToReceiveVms.addVirtualMachine(vm);
    }

    protected List<VirtualMachine> sortVmsInDescendingOrderBySimilarityWithHostCandidateToReceiveVms(Host hostToBeOffloaded, Host candidateToReceiveVms, double alpha,
            String similarityMethod) {
        List<VirtualMachine> vms = new ArrayList<>(hostToBeOffloaded.getVirtualMachines());
        List<VirtualMachine> sortedVirtualMachines = new ArrayList<>();

        List<ComputingResourceIdAndScore> sortedVmsIdAndScore = calculateSimilarity(candidateToReceiveVms, vms, alpha, similarityMethod);
        sortComputingResourceDowardScore(sortedVmsIdAndScore);

        for (ComputingResourceIdAndScore vmIdAndScore : sortedVmsIdAndScore) {
            for (VirtualMachine vm : vms) {
                if (vmIdAndScore.getId().equals(vm.getVmId())) {
                    sortedVirtualMachines.add(vm);
                }
            }
        }
        return sortedVirtualMachines;
    }

    protected double cosineSimilarity(long[] vmResourcesVector, long[] hostResourcesVector) {
        double sumOfProducts = 0;
        long sumOfVmResourcesVector = 0;
        long sumOfHostResourcesVector = 0;
        for (int i = 0; i < vmResourcesVector.length; i++) {
            sumOfProducts = sumOfProducts + (vmResourcesVector[i] * hostResourcesVector[i]);
            if (hostResourcesVector[i] == 0) {
                return 0;
            }
        }
        for (int j = 0; j < vmResourcesVector.length; j++) {
            sumOfVmResourcesVector = sumOfVmResourcesVector + (vmResourcesVector[j] * vmResourcesVector[j]);
            sumOfHostResourcesVector = sumOfHostResourcesVector + (hostResourcesVector[j] * hostResourcesVector[j]);
        }
        if (sumOfProducts == 0 || sumOfVmResourcesVector == 0 || sumOfHostResourcesVector == 0) {
            return 0;
        }
        return sumOfProducts / (Math.sqrt(sumOfVmResourcesVector) * Math.sqrt(sumOfHostResourcesVector));
    }

    protected double calculateCosize(double alfa, long[] vmResourcesVector, long[] hostResourcesVector) {
        double productOfVmResourcesVector = 1;
        for (int i = 0; i < vmResourcesVector.length; i++) {
            productOfVmResourcesVector = productOfVmResourcesVector * vmResourcesVector[i];
        }
        double cos = cosineSimilarity(vmResourcesVector, hostResourcesVector);
        return Math.pow(cos, alfa) * productOfVmResourcesVector;
    }

    protected double calculateCosineSimilarityWithHostVmRatio(double alfa, long[] vmResourcesVector, long[] hostResourcesVector) {
        double productOfVmResourcesVector = 1;
        double productOfHostResourcesVector = 1;
        for (int i = 0; i < vmResourcesVector.length; i++) {
            productOfVmResourcesVector = productOfVmResourcesVector * vmResourcesVector[i];
            productOfHostResourcesVector = productOfHostResourcesVector * hostResourcesVector[i];
        }
        double cos = cosineSimilarity(vmResourcesVector, hostResourcesVector);
        return Math.pow(cos, alfa) * (productOfHostResourcesVector / productOfVmResourcesVector);
    }

    private List<ComputingResourceIdAndScore> calculateSimilarity(Host candidateToReceiveVms, List<VirtualMachine> vms, double alpha, String similarityMethod) {
        List<ComputingResourceIdAndScore> scoredVms = new ArrayList<>();
        for (VirtualMachine vm : vms) {
            long vmCpuInMhz = vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores();
            long vmMemoryInMegaByte = vm.getVmServiceOffering().getMemoryInMegaByte();
            long[] vmConfigurationsVector = { vmCpuInMhz, vmMemoryInMegaByte };

            long candidateToReceiveVmsAvailableCpuInMhz = candidateToReceiveVms.getTotalCpuPowerInMhz() - candidateToReceiveVms.getCpuAllocatedInMhz();
            long candidateToReceiveVmsAvailableMemoryInMegaByte = candidateToReceiveVms.getTotalMemoryInMib() - candidateToReceiveVms.getMemoryAllocatedInMib();
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

    protected void calculateClusterUsage(List<Host> rankedHosts) {
        for (Host h : rankedHosts) {
            totalClusterCpuInMhz += h.getTotalCpuPowerInMhz();
            allocatedClusterCpuInMhz += h.getCpuAllocatedInMhz();
            totalClusterMemoryInMib += h.getTotalMemoryInMib();
            allocatedClusterMemoryInMib += h.getMemoryAllocatedInMib();
        }
        clusterCpuUsage = (double) allocatedClusterCpuInMhz / (double) totalClusterCpuInMhz;
        clusterMemoryUsage = (double) allocatedClusterMemoryInMib / (double) totalClusterMemoryInMib;
    }

    protected boolean isOverloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long maximumAcceptedAllocatedCpuInMhz = optimumAllocatedCpuInMhz(host) + vmsCpuProfile;
        long maximumAcceptedAllocatedMemoryInMib = optimumAllocatedMemoryInMib(host) + vmsMemoryProfile;

        if (host.getCpuAllocatedInMhz() >= maximumAcceptedAllocatedCpuInMhz) {
            return true;
        }
        if (host.getMemoryAllocatedInMib() >= maximumAcceptedAllocatedMemoryInMib) {
            return true;
        }
        return false;
    }

    protected boolean isUnderloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long minimumAcceptedAllocatedCpuInMhz = optimumAllocatedCpuInMhz(host) - vmsCpuProfile;
        long minimumAcceptedAllocatedMemoryInMib = optimumAllocatedMemoryInMib(host) - vmsMemoryProfile;

        if (host.getCpuAllocatedInMhz() < minimumAcceptedAllocatedCpuInMhz) {
            return true;
        }
        if (host.getMemoryAllocatedInBytes() < minimumAcceptedAllocatedMemoryInMib) {
            return true;
        }
        return false;
    }

    protected long optimumAllocatedCpuInMhz(Host host) {
        long optimumAllocatedCpuInMhz = (long) (clusterCpuUsage * host.getTotalCpuPowerInMhz());
        return optimumAllocatedCpuInMhz;
    }

    protected long optimumAllocatedMemoryInMib(Host host) {
        long optimumAllocatedMemoryInMib = (long) (clusterMemoryUsage * host.getTotalMemoryInMib());
        return optimumAllocatedMemoryInMib;
    }

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

        vmsAllocatedCpuInMhzMean = allocatedClusterCpuInMhz / numberOfVms;
        vmsAllocatedMemoryInMibMean = allocatedClusterMemoryInMib / numberOfVms;
    }

    protected void calculateVmsCpuAndMemoryMedian(Set<VirtualMachine> vms) {
        if (vms.size() == 0) {
            vmsAllocatedCpuInMhzMedian = 0;
            vmsAllocatedMemoryInMibMedian = 0;
            return;
        }
        List<Long> cpuValues = new ArrayList<>();
        List<Long> memoryValues = new ArrayList<>();
        int middle = vms.size() / 2;

        for (VirtualMachine vm : vms) {
            memoryValues.add(vm.getVmServiceOffering().getMemoryInMegaByte());
            cpuValues.add((long) vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores());
        }

        Collections.sort(cpuValues);
        Collections.sort(memoryValues);

        if (vms.size() % 2 == 1) {
            vmsAllocatedCpuInMhzMedian = cpuValues.get(middle);
            vmsAllocatedMemoryInMibMedian = memoryValues.get(middle);
        } else {
            vmsAllocatedCpuInMhzMedian = (cpuValues.get(middle - 1) + cpuValues.get(middle)) / 2;
            vmsAllocatedMemoryInMibMedian = (memoryValues.get(middle - 1) + memoryValues.get(middle)) / 2;
        }
    }

    /**
     * It sorts a list according hosts CPU usage and another for memory usage; a host score is the
     * sum of its index in those lists.
     */
    protected List<ComputingResourceIdAndScore> calculateHostsScore(List<ComputingResourceIdAndScore> hostsCpuUsageScore, List<ComputingResourceIdAndScore> hostsMemoryUsageScore) {
        List<ComputingResourceIdAndScore> hostsAgregatedScore = new ArrayList<>();
        for (int i = 0; i < hostsCpuUsageScore.size(); i++) {
            ComputingResourceIdAndScore h = new ComputingResourceIdAndScore(hostsCpuUsageScore.get(i).getId(), i);
            hostsAgregatedScore.add(h);
        }
        for (int i = 0; i < hostsMemoryUsageScore.size(); i++) {
            for (ComputingResourceIdAndScore cScore : hostsAgregatedScore) {
                if (cScore.getId().equals(hostsMemoryUsageScore.get(i).getId())) {
                    double score = cScore.getScore() + i;
                    cScore.setScore(score);
                    break;
                }
            }
        }
        return hostsAgregatedScore;
    }

    protected List<ComputingResourceIdAndScore> sortHostsByCpuUsage(List<Host> hostsToSortByCpuUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsToSortByCpuUsage) {
            long totalCpu = h.getTotalCpuPowerInMhz();
            long allocatedCpu = h.getCpuAllocatedInMhz();
            double cpuUsagePercentage = ((double) allocatedCpu / (double) totalCpu) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), cpuUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    /**
     * Sort {@link ComputingResourceIdAndScore} according to {@link Host} memory usage.
     */
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hostsSortedByMemoryUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsSortedByMemoryUsage) {
            long totalMemory = h.getTotalMemoryInMib();
            long allocatedMemory = h.getMemoryAllocatedInMib();
            double memoryUsagePercentage = ((double) allocatedMemory / (double) totalMemory) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), memoryUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    protected ComputingResourceIdAndScore createComputingResourceIdAndScore(String id, int score) {
        return new ComputingResourceIdAndScore(id, score);
    }

    /**
     * This class holds a computing resource id and its score.
     */
    public class ComputingResourceIdAndScore {
        private String id;
        private double score;

        public ComputingResourceIdAndScore(String id, double score) {
            this.id = id;
            this.score = score;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    /**
     * This class allows to compare Hosts upward based on each host score using the
     * {@link #compare(HostResources, HostResources)} method to return a integer, this class
     * implements {@link Comparator}.
     */
    public class ScoreUpwardComparator implements Comparator<ComputingResourceIdAndScore> {
        /**
         * Given 2 (two) {@link HostResources}, it returns the result from the first host score
         * subtracted to the second host score, allowing to compare both hosts. If the result is
         * greater than zero, then the first host score is bigger than the second host score; thus
         * the first host will be sorted in a lower index than the second host.
         */
        @Override
        public int compare(ComputingResourceIdAndScore c1, ComputingResourceIdAndScore c2) {
            //            return (int) (c1.getScore() - c2.getScore());
            if (c1.getScore() > c2.getScore())
                return 1;
            if (c1.getScore() < c2.getScore())
                return -1;
            else
                return 0;
        }

    }

    private ScoreUpwardComparator scoreUpwardComparator = new ScoreUpwardComparator();

    @SuppressWarnings("unchecked")
    private Comparator<ComputingResourceIdAndScore> reversedComparator = ComparatorUtils.reversedComparator(scoreUpwardComparator);

    /**
     * It sorts {@link ComputingResourceIdAndScore} by upward score. Computing resources with lower
     * score positioned on lower indexes of the list. It uses the
     * {@link Collections#sort(List, Comparator)}, where the comparator is the
     * {@link ScoreUpwardComparator}.
     */
    protected void sortComputingResourceUpwardScore(List<ComputingResourceIdAndScore> computingResourceIdAndScore) {
        Collections.sort(computingResourceIdAndScore, scoreUpwardComparator);
    }

    /**
     * It sorts {@link ComputingResourceIdAndScore} by downward score. Computing resources with
     * higher score are positioned on lower indexes of the list. It uses the
     * {@link Collections#sort(List, Comparator)}, where the comparator is the
     * {@link ScoreUpwardComparator}.
     */
    protected void sortComputingResourceDowardScore(List<ComputingResourceIdAndScore> computingResourceIdAndScore) {
        Collections.sort(computingResourceIdAndScore, reversedComparator);
    }

    protected List<Host> sortHostUpwardScoreBasedOnSortedListOfComputingResourceIdAndScore(List<ComputingResourceIdAndScore> computingResourceIdAndScore, List<Host> hosts) {
        List<Host> hostsSortedByScore = new ArrayList<>();
        for (ComputingResourceIdAndScore c : computingResourceIdAndScore) {
            for (Host h : hosts) {
                if (h.getId().equals(c.getId())) {
                    hostsSortedByScore.add(h);
                    break;
                }
            }
        }
        return hostsSortedByScore;
    }

    /**
     * Clones the given {@link List} of {@link Host} to a new list. The new list allows
     * to operate into same objects (from the cloned list) without alter the given list.
     */
    protected List<Host> cloneListOfHosts(List<Host> hosts) {
        List<Host> clonedHostList = new ArrayList<>();
        for (Host host : hosts) {
            try {
                Host clonedHost = (Host) host.clone();
                clonedHostList.add(clonedHost);
            } catch (CloneNotSupportedException e) {
                logger.error("Problems while clonning objects", e);
            }
        }
        return clonedHostList;
    }
}
