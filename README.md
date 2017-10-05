# Cluster VMs balancing through hosts and VMs similarity 

This project adds a set of heuristics capable of balancing VMs across hosts, avoiding service degradation caused by VMs competition. The proposed VMs balancing algorithm uses hosts and VMs similarities and is presented at the IEEE SERVICES 2017 conference.

## Cloud-traces
This is a branch from the project Cloud-traces. Cloud-traces simulates a cloud computing environment using Google data traces to emulate VMs workload and management heuristics behavior; source code, and documentation for using the Google data trace and extending management heuristics can be found <a href="https://github.com/Autonomiccs/cloud-traces">here</a>.

## VMs balancing algorithm and IEEE SERVICES 2017

This project has the algorithm developed for the conference paper submitted in IEEE SERVICES 2017, added in the Cloud-traces project. The VMs balancing algorithm is at the class ClusterVmsBalancingOrientedBySimilarity (br.com.autonomiccs.cloudTraces.algorithms.management.ClusterVmsBalancingOrientedBySimilarity).
 
## Usage
The usage can be summarized in the following steps:
* build the project;
* simulate the management algorithm;
* process log results.

#### Build project
To build the project you can use:

```
mvn clean compile package
```

#### Simulate the management algorithm

To execute a simulation you can simply run:
```
java -cp cloud-traces-1.0.0-SNAPSHOT-jar-with-dependencies.jar br.com.autonomiccs.cloudTraces.main.CloudTracesSimulator <pathTocloudVmTraces.csv>
```

or ...

```
java -cp cloud-traces-1.0.0-SNAPSHOT-jar-with-dependencies.jar br.com.autonomiccs.cloudTraces.main.CloudTracesSimulator <pathTocloudVmTraces.csv> empty
```

Note that the input with the algorithm name can be ignored (in this case it runs with ClusterVmsBalancingOrientedBySimilarity algorithm), or be "empty" for simulate the environment without a management algorithm;

#### Process log results

To process the log results you can run:

```
java -cp cloud-traces-1.0.0-SNAPSHOT-jar-with-dependencies.jar br.com.autonomiccs.cloudTraces.main.ProcessLogFileResults <pathToCloud-traces.log>
```

or ...

```
java -cp cloud-traces-1.0.0-SNAPSHOT-jar-with-dependencies.jar br.com.autonomiccs.cloudTraces.main.ProcessLogFileResults <pathToCloud-traces.log> <logType>
```

Note that the input for the log type can be one of the following:
* "mainLog": provides some statistics, this particular log was used for create both dispersion diagrams (number of VMs vs processing time, and number of VMs migrations vs processing time);
* "bestResults": this result will give a matrix of executions x algorithms. If a element is 0, then the respective heuristic did not achieved the best result at that iteration; if it is 1, then the respective heuristic achieved the best result at the iteration. This particular log was used to count the absolute wins of the compared parameters/heuristics;
* if this parameter is ignored, then it generates a log that can be used to plot graphs with Allocated CPU standard deviation, and Allocated memory standard deviation.


