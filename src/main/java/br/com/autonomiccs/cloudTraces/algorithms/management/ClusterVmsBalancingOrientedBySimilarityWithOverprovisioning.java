package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import br.com.autonomiccs.cloudTraces.algorithms.management.ClusterVmsBalancingOrientedBySimilarity.ComputingResourceIdAndScore;
import br.com.autonomiccs.cloudTraces.beans.Host;
import br.com.autonomiccs.cloudTraces.beans.VirtualMachine;

public class ClusterVmsBalancingOrientedBySimilarityWithOverprovisioning extends ClusterVmsBalancingOrientedBySimilarity {

    /**
     * TODO allocated resources provisioning factor. For instance, 0.8 means that a the algorithm
     * will considered the allocated resource as 80% of its original value; therefore, estimate that
     * its usage is at best of 80%.
     */
    private static final double ALLOCATED_CPU_PROVISIONING_FACTOR = 1;
    private static final double ALLOCATED_MEMORY_PROVISIONING_FACTOR = 1;

    private static final double CPU_OVERPROVISIONING = 2.0;
    private static final double MEMORY_OVERPROVISIONING = 2.0;

    protected double clusterCpuUsage, clusterMemoryUsage;
    private long totalClusterCpuInMhz = 0, allocatedClusterCpuInMhz = 0, totalClusterMemoryInMib = 0, allocatedClusterMemoryInMib = 0;
    protected long vmsAllocatedCpuInMhzMedian, vmsAllocatedMemoryInMibMedian, vmsAllocatedCpuInMhzMean, vmsAllocatedMemoryInMibMean;

    protected StandardDeviation std = new StandardDeviation(false);

    @Override
    protected void calculateClusterUsage(List<Host> rankedHosts) {
        for (Host h : rankedHosts) {
            totalClusterCpuInMhz += getTotalCpuInMhzWithOverprovisioning(h);
            totalClusterMemoryInMib += getTotalMmemoryInMibWithOverprovisioning(h);

            long allocatedCpuAfterProvisioningFactor = getAllocatedCpuInMhzWithProvisioningFactor(h);
            allocatedClusterCpuInMhz += allocatedCpuAfterProvisioningFactor;

            long allocatedMemoryAfterProvisioningFactor = getAllocatedMemoryInMibWithProvisioningFactor(h);
            allocatedClusterMemoryInMib += allocatedMemoryAfterProvisioningFactor;
        }
        clusterCpuUsage = (double) allocatedClusterCpuInMhz / (double) totalClusterCpuInMhz;
        clusterMemoryUsage = (double) allocatedClusterMemoryInMib / (double) totalClusterMemoryInMib;
    }

    @Override
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
            long allocatedCpuAfterProvisioningFactor = (long) (vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores()
                    * ALLOCATED_CPU_PROVISIONING_FACTOR);
            cpuValues.add(allocatedCpuAfterProvisioningFactor);

            long allocatedMemoryAfterProvisioningFactor = (long) (vm.getVmServiceOffering().getMemoryInMegaByte() * ALLOCATED_MEMORY_PROVISIONING_FACTOR);
            memoryValues.add(allocatedMemoryAfterProvisioningFactor);
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

    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByCpuUsage(List<Host> hostsToSortByCpuUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsToSortByCpuUsage) {
            long totalCpu = getTotalCpuInMhzWithOverprovisioning(h);
            long allocatedCpu = getAllocatedCpuInMhzWithProvisioningFactor(h);
            double cpuUsagePercentage = ((double) allocatedCpu / (double) totalCpu) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), cpuUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    /**
     * Sort {@link ComputingResourceIdAndScore} according to {@link Host} memory usage.
     */
    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hostsSortedByMemoryUsage) {
        List<ComputingResourceIdAndScore> hostsWithScore = new ArrayList<>();
        for (Host h : hostsSortedByMemoryUsage) {
            long totalMemory = getTotalMmemoryInMibWithOverprovisioning(h);
            long allocatedMemory = getAllocatedMemoryInMibWithProvisioningFactor(h);
            double memoryUsagePercentage = ((double) allocatedMemory / (double) totalMemory) * 100;
            hostsWithScore.add(new ComputingResourceIdAndScore(h.getId(), memoryUsagePercentage));
        }
        sortComputingResourceUpwardScore(hostsWithScore);
        return hostsWithScore;
    }

    @Override
    protected double calculateStandarDeviation(List<Host> hosts) {
        double memoryUsage[] = new double[hosts.size()];
        double cpuUsage[] = new double[hosts.size()];

        for (int i = 0; i < hosts.size(); i++) {
            memoryUsage[i] = getAllocatedMemoryInMibWithProvisioningFactor(hosts.get(i)) / getTotalMmemoryInMibWithOverprovisioning(hosts.get(i));
            cpuUsage[i] = getAllocatedCpuInMhzWithProvisioningFactor(hosts.get(i)) / getTotalCpuInMhzWithOverprovisioning(hosts.get(i));
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

    protected List<ComputingResourceIdAndScore> calculateSimilarity(Host candidateToReceiveVms, List<VirtualMachine> vms, double alpha, String similarityMethod) {
        List<ComputingResourceIdAndScore> scoredVms = new ArrayList<>();
        for (VirtualMachine vm : vms) {
            long vmCpuInMhz = vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores();
            long vmMemoryInMegaByte = vm.getVmServiceOffering().getMemoryInMegaByte();
            long[] vmConfigurationsVector = { vmCpuInMhz, vmMemoryInMegaByte };

            long candidateToReceiveVmsAvailableCpuInMhz = getTotalCpuInMhzWithOverprovisioning(candidateToReceiveVms)
                    - getAllocatedCpuInMhzWithProvisioningFactor(candidateToReceiveVms);
            long candidateToReceiveVmsAvailableMemoryInMegaByte = getTotalMmemoryInMibWithOverprovisioning(candidateToReceiveVms)
                    - getAllocatedMemoryInMibWithProvisioningFactor(candidateToReceiveVms);
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
    protected void updateHostResources(Host hostToBeOffloaded, Host candidateToReceiveVms, VirtualMachine vm) {
        long vmCpuConfigurationInMhz = (long) (vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores() * ALLOCATED_CPU_PROVISIONING_FACTOR);
        long vmMemoryConfigurationInBytes = (long) (vm.getVmServiceOffering().getMemoryInMegaByte() * NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE * ALLOCATED_MEMORY_PROVISIONING_FACTOR);

        hostToBeOffloaded.getVirtualMachines().remove(vm);
        hostToBeOffloaded.setCpuAllocatedInMhz(getAllocatedCpuInMhzWithProvisioningFactor(hostToBeOffloaded) - vmCpuConfigurationInMhz);
        hostToBeOffloaded.setMemoryAllocatedInBytes(getAllocatedMemoryInMibWithProvisioningFactor(hostToBeOffloaded) - vmMemoryConfigurationInBytes);

        candidateToReceiveVms.addVirtualMachine(vm);
    }

    @Override
    protected boolean isOverloaded(Host host, long vmsCpuProfile, long vmsMemoryProfile) {
        long maximumAcceptedAllocatedCpuInMhz = optimumAllocatedCpuInMhz(host) + vmsCpuProfile;
        long maximumAcceptedAllocatedMemoryInMib = optimumAllocatedMemoryInMib(host) + vmsMemoryProfile;

        if (getAllocatedCpuInMhzWithProvisioningFactor(host) >= maximumAcceptedAllocatedCpuInMhz) {
            return true;
        }
        if (getAllocatedMemoryInMibWithProvisioningFactor(host) >= maximumAcceptedAllocatedMemoryInMib) {
            return true;
        }
        return false;
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
    protected double cosineSimilarity(long[] vmResourcesVector, long[] hostResourcesVector) {
        long sumOfProducts = 0;
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

    @Override
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

    @Override
    protected long optimumAllocatedCpuInMhz(Host host) {
        long optimumAllocatedCpuInMhz = (long) (clusterCpuUsage * getTotalCpuInMhzWithOverprovisioning(host));
        return optimumAllocatedCpuInMhz;
    }

    @Override
    protected long optimumAllocatedMemoryInMib(Host host) {
        long optimumAllocatedMemoryInMib = (long) (clusterMemoryUsage * getTotalMmemoryInMibWithOverprovisioning(host));
        return optimumAllocatedMemoryInMib;
    }

    private long getAllocatedCpuInMhzWithProvisioningFactor(Host host) {
        return (long) (host.getCpuAllocatedInMhz() * ALLOCATED_CPU_PROVISIONING_FACTOR);
    }

    private long getTotalCpuInMhzWithOverprovisioning(Host host) {
        return (long) (host.getTotalCpuPowerInMhz() * CPU_OVERPROVISIONING);
    }

    private long getAllocatedMemoryInMibWithProvisioningFactor(Host host) {
        return (long) (host.getMemoryAllocatedInMib() * ALLOCATED_MEMORY_PROVISIONING_FACTOR);
    }

    private long getTotalMmemoryInMibWithOverprovisioning(Host host) {
        return (long) (host.getTotalMemoryInMib() * MEMORY_OVERPROVISIONING);
    }

}
