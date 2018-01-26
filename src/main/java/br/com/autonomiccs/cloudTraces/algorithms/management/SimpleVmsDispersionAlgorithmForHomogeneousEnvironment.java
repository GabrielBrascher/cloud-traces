package br.com.autonomiccs.cloudTraces.algorithms.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.util.MathUtils;
import org.apache.log4j.Logger;

import br.com.autonomiccs.cloudTraces.algorithms.management.ClusterVmsBalancingOrientedBySimilarity.ComputingResourceIdAndScore;
import br.com.autonomiccs.cloudTraces.beans.Host;
import br.com.autonomiccs.cloudTraces.beans.VirtualMachine;

public class SimpleVmsDispersionAlgorithmForHomogeneousEnvironment extends ClusterVmsBalancingOrientedBySimilarity {

    private final static Logger logger = Logger.getLogger(SimpleVmsDispersionAlgorithmForHomogeneousEnvironment.class);

    /**
     * Represents the sum of hosts used memory divided by the sum of hosts total memory.
     */
    private double clusterMemoryUsageAverage;

    protected static final long NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE = 1024l;

    private StandardDeviation std = new StandardDeviation(false);

    /**
     * Standard deviation of VMs memory configuration. This value considers scenarios where there
     * are a high variability in the VMs amount of resources, given the maximum and minimum load
     * allowed considering that some VMs might be big.
     */
    private double standardDeviationVmsConfiguration;

    /**
     * Standard deviation of hosts memory usage. This value considers the heterogeneous usage of
     * resources in the hosts, if the usage is very different, it will have a higher standard
     * deviation and allows the maximum and minimum load based on this evaluation.
     */
    private double standardDeviationHostsUsage;

    /**
     * It is the average between the {@link #standardDeviationVmsConfiguration} and
     * {@link #standardDeviationHostsUsage} standard deviation values.
     */
    private double standardDeviationAverage;

    /**
     * It ranks hosts to receive VMs, setting each host score using {@link #setEachHostScore(List)}
     * and sorts the list using {@link #sortHosts(List)} method.
     */
    @Override
    public List<Host> rankHosts(List<Host> hostsList) { //TODO revisar
        clusterMemoryUsagePercentage(hostsList);
        return sortHostUpwardScoreBasedOnSortedListOfComputingResourceIdAndScore(sortHostsByMemoryUsage(hostsList), hostsList);
    }

    /*  clusterMemoryUsagePercentage(hostsList);
        setEachHostScore(hostsList);
        sortHosts(hostsList);
        return hostsList;
     */

    /**
     * It maps VMs from hosts with lower score (interest in distribute some of its VMs) to hosts
     * with
     * higher score (interesting to allocate more VMs). This methods simulates three different
     * mappings methods; the simulation that give the best standard deviation on the hosts workload
     * will be the chosen one. Each simulation is done by calling the {@link simulateVmsMigrations}
     * method with a different standard deviation ({@link #standardDeviationVmsConfiguration},
     * {@link #standardDeviationHostsUsage}, {@link #standardDeviationAverage}).
     * The standard deviation is used to give a maximum load in each host (host average load +
     * standard deviation) and the minimum load (host average load - standard deviation).
     */
    @Override
    public Map<VirtualMachine, Host> mapVMsToHost(List<Host> rankedHosts) {
        if (MathUtils.equals(standardDeviationHostsUsage, 0)) {
            return new HashMap<>();
        }
        List<Host> rankedHostsStdVms = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsStdHosts = cloneListOfHosts(rankedHosts);
        List<Host> rankedHostsStdAverage = cloneListOfHosts(rankedHosts);

        logger.debug("Simulating migration mapping with VMs resource standard deviation");
        Map<VirtualMachine, Host> vmsToHostStdVms = simulateVmsMigrations(rankedHostsStdVms, standardDeviationVmsConfiguration);

        logger.debug("Simulating migration mapping with hosts resource standard deviation");
        Map<VirtualMachine, Host> vmsToHostStdHosts = simulateVmsMigrations(rankedHostsStdHosts, standardDeviationHostsUsage);

        logger.debug("Simulating migration mapping with the average of hosts resource and VMs resource standard deviation");
        Map<VirtualMachine, Host> vmsToHostStdAverage = simulateVmsMigrations(rankedHostsStdAverage, standardDeviationAverage);

        double usedMemoryHostsWithStdVms[] = new double[rankedHosts.size()];
        for (int i = 0; i < rankedHostsStdVms.size(); i++) {
            usedMemoryHostsWithStdVms[i] = rankedHostsStdVms.get(i).getMemoryAllocatedInBytes();
        }
        double stdWithStdVms = std.evaluate(usedMemoryHostsWithStdVms);
        logger.debug(String.format("The Std. achieved using the VMs resource standard deviation as parameter for the simulation was [%f]", stdWithStdVms));
        double usedMemoryHostsWithStdHost[] = new double[rankedHosts.size()];
        for (int i = 0; i < rankedHostsStdHosts.size(); i++) {
            usedMemoryHostsWithStdHost[i] = rankedHostsStdHosts.get(i).getMemoryAllocatedInBytes();
        }
        double stdWithStdHosts = std.evaluate(usedMemoryHostsWithStdHost);
        logger.debug(String.format("The Std. achieved using the hosts resource standard deviation as parameter for the simulation was [%f]", stdWithStdHosts));

        double usedMemoryHostsWithStdAverage[] = new double[rankedHosts.size()];
        for (int i = 0; i < rankedHostsStdAverage.size(); i++) {
            usedMemoryHostsWithStdAverage[i] = rankedHostsStdAverage.get(i).getMemoryAllocatedInBytes();
        }
        double stdWithStdAverage = std.evaluate(usedMemoryHostsWithStdAverage);
        logger.debug(String.format("The Std. achieved using the average between hosts resource and VMs resource standard deviation as parameter for the simulation was [%f]",
                stdWithStdAverage));

        if (stdWithStdAverage <= stdWithStdHosts && stdWithStdAverage <= stdWithStdVms) {
            logger.debug("The simulation that won the competition was the one executed with alpha as the average of host resource and VMs resource standard deviation.");
            logger.debug(String.format("The number of migrations that will be executed is[%d].", vmsToHostStdAverage.size()));
            return vmsToHostStdAverage;
        }
        if (stdWithStdHosts <= stdWithStdVms) {
            logger.debug("The simulation that won the competition was the one executed with alpha as the host resource standard deviation.");
            logger.debug(String.format("The number of migrations that will be executed is[%d].", vmsToHostStdHosts.size()));
            return vmsToHostStdHosts;
        }
        logger.debug("The simulation that won the competition was the one executed with alpha as the VMs resource standard deviation.");
        logger.debug(String.format("The number of migrations that will be executed is[%d].", vmsToHostStdVms.size()));
        return vmsToHostStdVms;
    }

    /**
     * It updates the cluster memory usage average, and the standard deviations (
     * {@link #clusterMemoryUsageAverage}, {@link #standardDeviationAverage},
     * {@link #standardDeviationHostsUsage}, {@link #standardDeviationVmsConfiguration})
     */
    protected void clusterMemoryUsagePercentage(List<Host> hosts) {
        double clusterUsedMemory = 0;
        double vmsUsedMemory[] = null;
        double hostsUsedMemory[] = new double[hosts.size()];
        for (int hostIterator = 0; hostIterator < hosts.size(); hostIterator++) {
            Set<VirtualMachine> vmsResources = hosts.get(hostIterator).getVirtualMachines();
            double hostVmsUsedMemory[] = new double[vmsResources.size()];
            int vmIterator = 0;
            for (VirtualMachine vm : vmsResources) {
                hostVmsUsedMemory[vmIterator] = vm.getVmServiceOffering().getMemoryInMegaByte();
                vmIterator++;
            }
            vmsUsedMemory = ArrayUtils.addAll(vmsUsedMemory, hostVmsUsedMemory);

            long hostUsedMemoryInMegaBytes = hosts.get(hostIterator).getMemoryAllocatedInBytes();
            hostsUsedMemory[hostIterator] = hostUsedMemoryInMegaBytes;
            clusterUsedMemory += hostUsedMemoryInMegaBytes;
        }
        clusterMemoryUsageAverage = clusterUsedMemory / hosts.size();
        standardDeviationVmsConfiguration = std.evaluate(vmsUsedMemory);
        standardDeviationHostsUsage = std.evaluate(hostsUsedMemory);
        standardDeviationAverage = (standardDeviationVmsConfiguration + standardDeviationHostsUsage) / 2;
    }

    /**
     * Sort {@link ComputingResourceIdAndScore} according to {@link Host} memory usage.
     */
    @Override
    protected List<ComputingResourceIdAndScore> sortHostsByMemoryUsage(List<Host> hosts) {
        List<ComputingResourceIdAndScore> hostsSortedByMemoryUsage = new ArrayList<>();
        for (Host h : hosts) {
            hostsSortedByMemoryUsage.add(new ComputingResourceIdAndScore(h.getId(), calculateHostScore(h)));
        }
        //        sortComputingResourceUpwardScore(hostsSortedByMemoryUsage); //TODO Upward ou Downward
        sortComputingResourceDowardScore(hostsSortedByMemoryUsage);
        return hostsSortedByMemoryUsage;
    }

    protected double calculateHostScore(Host host) {
        if (host.getMemoryAllocatedInBytes() != 0) {
            return host.getTotalMemoryInBytes() / (host.getMemoryAllocatedInBytes() * 1d);
        }
        return Double.POSITIVE_INFINITY;
    }

    /**
     * For a given a list of {@link Host} and a value that represents the standard
     * deviation it maps virtual machine migrations. The standard deviation is used as a
     */
    protected Map<VirtualMachine, Host> simulateVmsMigrations(List<Host> rankedHosts, double standardDeviation) {
        Map<VirtualMachine, Host> vmsToHost = new HashMap<>();

        double hostMemoryMinimumUsageAllowed = clusterMemoryUsageAverage - standardDeviation;
        for (int i = rankedHosts.size() - 1; i > 0; i--) {
            Host hostToBeOffLoaded = rankedHosts.get(i);
            if (hostToBeOffLoaded.getMemoryAllocatedInBytes() > hostMemoryMinimumUsageAllowed) {

                for (Host hostCandidateToReceiveVMs : rankedHosts) {
                    if (hostToBeOffLoaded == hostCandidateToReceiveVMs) {
                        continue;
                    }
                    if (hostCandidateToReceiveVMs.getMemoryAllocatedInBytes() <= clusterMemoryUsageAverage) {
                        Set<VirtualMachine> clonedListOfVms = cloneHostVmsList(hostToBeOffLoaded);
                        for (VirtualMachine vmResources : clonedListOfVms) {
                            if (isMemoryUsageOfHostsAfterVmMigration(vmResources, hostToBeOffLoaded, hostCandidateToReceiveVMs, standardDeviation)
                                    && canMigrateVmToHost(vmResources, hostCandidateToReceiveVMs)) {
                                updateHostUsedResources(vmResources, hostToBeOffLoaded, hostCandidateToReceiveVMs);
                                vmsToHost.put(vmResources, hostCandidateToReceiveVMs);
                            }
                        }
                    }
                }
            }
        }
        return vmsToHost;
    }

    /**
     * It returns true if a VM migration will maintain the VM's current host with a usage above the
     * {@link #hostMemoryMinimumUsageAllowedStdVms}; and if the target host usage will stay below
     * the {@link #hostMemoryMaximumUsageAllowedStdVms} after the migration.
     */
    private boolean isMemoryUsageOfHostsAfterVmMigration(VirtualMachine vmResources, Host hostVmResidOn, Host targetHost, double standarDeviation) {
        if (hostVmResidOn.getMemoryAllocatedInBytes() == targetHost.getMemoryAllocatedInBytes()) {
            return false;
        }
        long vmMemoryInMegaBytes = vmResources.getVmServiceOffering().getMemoryInMegaByte();
        long hostMemoryAfterMigrateVm = hostVmResidOn.getMemoryAllocatedInBytes() - vmMemoryInMegaBytes;
        long targetHostMemoryUsageAfterReceiveVM = targetHost.getMemoryAllocatedInBytes() + vmMemoryInMegaBytes;

        double hostMemoryMinimumUsageAllowed = clusterMemoryUsageAverage - standarDeviation;
        double hostMemoryMaximumUsageAllowed = clusterMemoryUsageAverage + standarDeviation;

        return hostMemoryMinimumUsageAllowed <= hostMemoryAfterMigrateVm && targetHostMemoryUsageAfterReceiveVM < hostMemoryMaximumUsageAllowed;
    }

    /**
     * It clones a list of {@link VirtualMachineResource}.
     */
    private Set<VirtualMachine> cloneHostVmsList(Host host) {
        return new HashSet<>(host.getVirtualMachines());
    }

    /**
     * Checks if the host can allocate the VM. If the given host has more resources available than
     * the resources needed by the VM, it returns true. The comparison of resources considers the
     * number of CPUs, total CPU (number of CPUs * CPU speed) and total memory; if any comparison
     * fails it returns false.
     */
    private boolean canMigrateVmToHost(VirtualMachine vm, Host host) {
        long vmCpuNeeds = vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores();
        if (getHostAvailableCpu(host) < vmCpuNeeds) {
            return false;
        }
        long vmMemory = vm.getVmServiceOffering().getMemoryInMegaByte();
        long hostAvailableMemory = getHostAvailableMemory(host);
        if (hostAvailableMemory < vmMemory) {
            return false;
        }
        return true;
    }

    private float getHostAvailableCpu(Host host) {
        return host.getTotalCpuPowerInMhz() - host.getCpuAllocatedInMhz();
    }

    /**
     * Returns the amount of memory in the given {@link Host}. It already considers the memory
     * over-provisioning
     */
    private long getHostAvailableMemory(Host host) {
        return host.getTotalMemoryInBytes() - host.getMemoryAllocatedInBytes();
    }

    /**
     * It updates the resources counting (used cpu and memory) from the host that the VM resids on
     * and the target host to be migrated.
     */
    protected void updateHostUsedResources(VirtualMachine vm, Host hostVmResidOn, Host targetHost) {
        targetHost.addVirtualMachine(vm);

        hostVmResidOn.getVirtualMachines().remove(vm);
        hostVmResidOn.setCpuAllocatedInMhz(hostVmResidOn.getCpuAllocatedInMhz() - (vm.getVmServiceOffering().getCoreSpeed() * vm.getVmServiceOffering().getNumberOfCores()));
        hostVmResidOn.setMemoryAllocatedInBytes(hostVmResidOn.getMemoryAllocatedInBytes() - (vm.getVmServiceOffering().getMemoryInMegaByte() * NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE));
    }

}
