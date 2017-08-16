/*
 * Cloud traces
 * Copyright (C) 2016 Autonomiccs, Inc.
 *
 * Licensed to the Autonomiccs, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The Autonomiccs, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package br.com.autonomiccs.cloudTraces.beans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Host extends ComputingResource {

    private Set<VirtualMachine> virtualMachines = new HashSet<>();
    private String clusterId;
    private long predictedCpuUsageInMhz;
    private long predictedMemoryUsageInMib;
    private int sampleSize; //TODO enhance variable name
    List<Long> cpuUsageInMhzSample = new ArrayList<>();
    List<Long> memoryUsageInMibSample = new ArrayList<>();

    public Host(String id) {
        super(id);
    }

    public Set<VirtualMachine> getVirtualMachines() {
        return virtualMachines;
    }

    public long getPredictedCpuUsageInMhz() {
        return predictedCpuUsageInMhz;
    }

    public long getPredictedMemoryUsageInMib() {
        return predictedMemoryUsageInMib;
    }

    /**
     * TODO
     */
    public void updateHostUsagePrediction() {
        updateUsageSamples();

        long sumOfCpuUsageInMhz = 0;
        long sumOfMemoryUsageInMib = 0;

        for (int i = cpuUsageInMhzSample.size() - 1; i > 0; i--) {
            sumOfCpuUsageInMhz += cpuUsageInMhzSample.get(i);
            sumOfMemoryUsageInMib += memoryUsageInMibSample.get(i);
        }

        sumOfCpuUsageInMhz = sumOfCpuUsageInMhz / cpuUsageInMhzSample.size();
        sumOfMemoryUsageInMib = sumOfMemoryUsageInMib / cpuUsageInMhzSample.size();
        predictedCpuUsageInMhz = (predictedMemoryUsageInMib + sumOfCpuUsageInMhz) / 2;
        predictedMemoryUsageInMib = (predictedMemoryUsageInMib + sumOfMemoryUsageInMib) / 2;
    }

    /**
     * It removes the oldest CPU and Memory usage in the sample of resources usage and includes new
     * usage metrics.
     */
    private void updateUsageSamples() {
        if (cpuUsageInMhzSample.size() < sampleSize) {
            cpuUsageInMhzSample.add(this.getCpuUsedInMhz());
            memoryUsageInMibSample.add(this.getMemoryUsedInBytes());
        } else {
            cpuUsageInMhzSample.remove(0);
            cpuUsageInMhzSample.add(this.getCpuUsedInMhz());

            memoryUsageInMibSample.remove(0);
            memoryUsageInMibSample.add(this.getMemoryUsedInBytes());
        }
    }

    public void addVirtualMachine(VirtualMachine vm) {
        virtualMachines.add(vm);
        vm.setHost(this);

        VmServiceOffering vmServiceOffering = vm.getVmServiceOffering();
        long vmRequestedCpu = getVmRequestedCpu(vmServiceOffering);

        setCpuAllocatedInMhz(getCpuAllocatedInMhz() + vmRequestedCpu);
        setMemoryAllocatedInBytes(getMemoryAllocatedInBytes() + getVmRquestedMemoryInBytes(vmServiceOffering));
    }

    private long getVmRquestedMemoryInBytes(VmServiceOffering vmServiceOffering) {
        return vmServiceOffering.getMemoryInMegaByte() * NUMBER_OF_BYTES_IN_ONE_MEGA_BYTE;
    }

    public void destroyVirtualMachine(VirtualMachine vm) {
        boolean removedVm = virtualMachines.remove(vm);
        assert removedVm;

        VmServiceOffering vmServiceOffering = vm.getVmServiceOffering();
        long vmRequestedCpu = getVmRequestedCpu(vmServiceOffering);

        setCpuAllocatedInMhz(getCpuAllocatedInMhz() - vmRequestedCpu);
        setMemoryAllocatedInBytes(getMemoryAllocatedInBytes() - getVmRquestedMemoryInBytes(vmServiceOffering));

        vm.setHost(null);
    }

    private long getVmRequestedCpu(VmServiceOffering vmServiceOffering) {
        return vmServiceOffering.getCoreSpeed() * vmServiceOffering.getNumberOfCores();
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public String toString() {
        return String.format("clusterId[%s] Host %s, #virtualMachines[%d]", clusterId, super.toString(), virtualMachines.size());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Host) {
            return ((Host)obj).getId().equals(this.getId());
        }
        return false;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Host clone = (Host)super.clone();
        clone.virtualMachines = new HashSet<>(virtualMachines);
        return clone;
    }
}
