<!--
~ Copyright (c) Imply Data, Inc. All rights reserved.
~
~ This software is the confidential and proprietary information
~ of Imply Data, Inc. You shall not disclose such Confidential
~ Information and shall use it only in accordance with the terms
~ of the license agreement you entered into with Imply.
-->

# Imply Extra Service Dimensions

An Extension that injects additional service dimensions that should be emitted along with service events.

## Extra dimensions injected

Future changes may look to introduce other task properties as dimensions

### Peons

- `task_id`: The id of the task that is running on the peon.
- `group_id`: The id of the group that the task running on the peon is part of.
- `data_source`: The data source that the task is being run against.

### Other Druid nodes

None

## Enabling the extension

To enable the extension, add the extension to the load list in the common runtime properties (or for a specific node)
```
druid.extensions.loadList=["druid-lookups-cached-global","druid-datasketches","imply-extra-service-dimensions"]
```