<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Imply Manager Autoscaler

Autoscaler for your Druid ingestion service running on Imply Manager

## To load the extension

Add `imply-manager-autoscaler` to the extensions load list

```json
druid.extensions.loadList=["druid-lookups-cached-global","druid-datasketches","imply-manager-autoscaler"]
...
```

## How it works

Druid has an AutoScaler framework that allows Extension to be built on top of it. 
This extension is built on top of the above mentioned framework and allow Druid to scale ingestion services 
based on provision strategy when Druid is running on Imply Manager. The Auto Scaler extension framework basically has 
2 recurring jobs for provision and termination of middleManagers. The  Auto scaler will adjusting the number of middleManagers 
to be between minNumWorkers and maxNumWorkers while taking into account number of ingestion tasks running and pending based on provisioning strategy. 
There are currently 2 provisioning strategies, pendingTaskBased and simple. 
Simple provisioning strategy scale up one node at a time based on if there are pending task. 
PendingTaskBased provisioning strategy provision new middleManagers to run all pending tasks in the queue based on workerSelectStrategy 
and terminate existing middleManagers that 1) are idle for timeout or 2) have a lower version than the minimum version configured 
(MiddleManagers to terminate are marked as lazy in the overlord before they are terminated, so that the overlord stop assigning new tasks to them).

### Configuration

#### In Overlord dynamic config:
```
{
  "selectStrategy": {
    "type": "fillCapacity"
  },
  "autoScaler": {
    "envConfig" : {
      "implyManagerAddress" : <imply_manager_address>,
      "clusterId" : <cluster_id>
    },
    "maxNumWorkers" : <max_num_worker>,
    "minNumWorkers" : <min_num_worker>,
    "type" : "implyManager"
  }
}
```
| property | description | requried? | default |
| --- | --- | --- | --- |
| `implyManagerAddress` | Address to the Imply Manager service  | yes | None. |
| `clusterId` | Id of Druid cluster in Imply Manager | yes | None. |
| `maxNumWorkers` | Maximum number of ingestion service instances | yes | None. |
| `minNumWorkers` | Minimum number of ingestion service instances | yes | None. |
| `type` | Must be set to implyManager. | yes | None

#### In middleManager/runtime.properties:
| property | description | requried? | default |
| --- | --- | --- | --- |
| `druid.worker.version` | Worker version of ingestion service provided by imply manager  | yes | None. |

#### In overlord/runtime.properties:
| property | description | requried? | default |
| --- | --- | --- | --- |
| `druid.indexer.runner.minWorkerVersion` | Worker version of ingestion service provided by imply manager  | yes | None. |
| `druid.indexer.autoscale.workerVersion` | Worker version of ingestion service provided by imply manager | yes | None. |
| `druid.indexer.autoscale.doAutoscale` | If set to "true" autoscaling will be enabled.  | no | false |
| `druid.indexer.autoscale.workerPort` | The port that MiddleManagers will run on. | no | 8080. |

Note that the configurations in middleManager and in overlord should be set by Imply Manager when Imply Manager provision the instances.
