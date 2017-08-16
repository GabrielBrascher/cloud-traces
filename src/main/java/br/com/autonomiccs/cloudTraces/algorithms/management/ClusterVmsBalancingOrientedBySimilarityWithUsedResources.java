package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import br.com.autonomiccs.cloudTraces.beans.Host;

public class ClusterVmsBalancingOrientedBySimilarityWithUsedResources extends ClusterVmsBalancingOrientedBySimilarity {

    private List<Host> originalManagedHostsList;
    private Map<String, List<Long>> mapOfUsedResources;

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
    protected List<ComputingResourceIdAndScore> sortHostsByCpuUsage(List<Host> hostsToSortByCpuUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsToSortByCpuUsage) {
            h.updateHostUsagePrediction();

            long totalCpu = h.getTotalCpuPowerInMhz();
            long allocatedCpu = h.getCpuAllocatedInMhz();
            long predictedCpuUsageInMhz = h.getPredictedCpuUsageInMhz();
            double cpuUsagePercentage = ((double) predictedCpuUsageInMhz / (double) totalCpu) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), cpuUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hostsSortedByMemoryUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsSortedByMemoryUsage) {
            long totalMemory = h.getTotalMemoryInMib();
            long allocatedMemory = h.getMemoryAllocatedInMib();
            long predictedMemoryUsageinMib = h.getPredictedMemoryUsageInMib();
            double memoryUsagePercentage = ((double) predictedMemoryUsageinMib / (double) totalMemory) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), memoryUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected boolean isOverloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long maximumAcceptedCpuUsageInMhz = optimumAllocatedCpuInMhz(host) + vmsCpuProfile;
        long maximumAcceptedMemoryUsageInMib = optimumAllocatedMemoryInMib(host) + vmsMemoryProfile;

        if (host.getPredictedCpuUsageInMhz() >= maximumAcceptedCpuUsageInMhz) {
            return true;
        }
        if (host.getPredictedMemoryUsageInMib() >= maximumAcceptedMemoryUsageInMib) {
            return true;
        }
        return false;
    }

    @Override
    protected boolean isUnderloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long minimumAcceptedCpuUsageInMhz = optimumAllocatedCpuInMhz(host) - vmsCpuProfile;
        long minimumAcceptedMemoryUsageInMib = optimumAllocatedMemoryInMib(host) - vmsMemoryProfile;

        if (host.getPredictedCpuUsageInMhz() < minimumAcceptedCpuUsageInMhz) {
            return true;
        }
        if (host.getPredictedMemoryUsageInMib() < minimumAcceptedMemoryUsageInMib) {
            return true;
        }
        return false;
    }

}
