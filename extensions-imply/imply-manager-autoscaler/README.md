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
2 recurring jobs for provision and termination of ingestion services. The Auto scaler will be adjusting the number of ingestion services 
to be between minNumWorkers and maxNumWorkers while taking into account number of ingestion tasks running and pending based on provisioning strategy. 
There are currently 2 provisioning strategies, pendingTaskBased and simple. 
Simple provisioning strategy scales up one node at a time based on if there are pending task. 
PendingTaskBased provisioning strategy provision new ingestion services to run all pending tasks in the queue based on workerSelectStrategy 
and terminate existing ingestion services that 1) are idle for timeout or 2) have a lower version than the minimum version configured 
(ingestion service to terminate are marked as lazy in the overlord before they are terminated, so that the overlord stop assigning new tasks to them).

This extension currently only works with the SaaS variant of Imply Manager. 

The extension relies on the following APIs of Imply Manager:
- POST https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances
- DELETE https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances/{instanceId}
- GET https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances

More details on the API contracts can be found in [below section](#imply-manager-api-contract).

Note that SaaS variant of Imply Manager uses keycloak for authentication. Please refer to README.md of imply-keycloak extension for setup details.

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
      "clusterId" : <cluster_id>,
      "useHttps" : <true/false>
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
| `useHttps` | Setting to true will make Druid uses https for Imply Manager endpoints, while setting to false will make Druid uses http for Imply Manager endpoints.| no | false |
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

## Imply Manager API contract

### Provision new instance

Creates instances for this druid cluster. This API return success (200) when Imply manager start creating the requested instance. The underlying actions of the instance being created and running take additional time. You must separately verify the result with the listInstances method.
Note that this API may be call multiple times consecutively before instances from earlier requests are ready/started/running. (This is because of how the existing Druid code was built. Druid basically make a call to provision 1 instances in a loop until it reach the desired number of instances in the cluster, instead of sending the desired number of instances in the API)

`POST https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances`

Path Param:
- `implyManagerAddress` : Address to the imply manager service
- `druidClusterId` : Id of Druid cluster to create instance

Request Body:
```
{
      "numToCreate": 1,
      "version": <worker_version>
}
```
- `numToCreate`: Number of instance to create (required)
- `version`: Worker version of instance to create (required). Worker version is map to Druid version by Imply manager and worker version will be set on the newly launched instance `druid.worker.version` property.

Response Body:
```
{
  "instanceIds": [<instance_id_1>,...]
}
```
- `instanceIds` : id for instance created

Status Code:
- 200: Imply manager successfully issue CREATE instance command
- 400: Invalid request / invalid version
- 403: Invalid permission to Druid cluster id

### Terminate instances:

Flags the specified instance in this druid cluster for immediate deletion. This API return success (200) when Imply manager successfully flag the requested instance for deletion. The underlying actions of the instance being deleted and shutting down take additional time. You must separately verify the result with the listInstances method.

`DELETE https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances/{instanceId}`

Path Param:
- `implyManagerAddress` : Address to the imply manager service 
- `druidClusterId` : Id of Druid cluster to create instance

Request Body:
None

Response Body:
None

Status Code:
- 200: Imply manager successfully issue delete instance command
- 403: Invalid permission to Druid cluster id

### List instances:

Lists all of the ingestion service instances in this druid cluster.  Note: this API will return all instances in a single response object (no pagination).

`GET https://{implyManagerAddress}/manager/v1/autoscaler/{druidClusterId}/instances`

Path Param:
- `implyManagerAddress` : Address to the imply manager service 
- `druidClusterId` : Id of Druid cluster to create instance

Request Body:
None

Response Body:
```
{
  "instances": [<instance_object>]
}
```
instance_object:
```
{
  "status": "<instance_status>"
  "ip": "<ip_of_instance>",
  "id": "<id_of_instance>"
}
```
- `status`: Status of the instance. Possible values are STARTING, RUNNING, TERMINATING
- `ip`: ip of instance
- `id`: instance id

Status Code:
- 200: Query success
- 403: Invalid permission to Druid cluster id
