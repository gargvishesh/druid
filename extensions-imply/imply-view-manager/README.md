<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# SQL View Manager

SQL Views, for your Druid cluster!

Add `imply-view-manager` to the extensions load list, and set `druid.sql.viewmanager.type` to `imply`.

```json
druid.extensions.loadList=["druid-lookups-cached-global","druid-datasketches","imply-view-manager"]
...

druid.sql.viewmanager.type=imply
...
```

## How it works

SQL view definitions are managed by the Druid Coordinator, which operates as a 'view manager' and stores these view definitions in the SQL metadata store in a new table, `druid_views`.

```java
StringUtils.format(
    "CREATE TABLE %1$s (\n"
    + "  view_name VARCHAR(255) NOT NULL,\n"
    + "  view_namespace VARCHAR(255) NOT NULL,\n"
    + "  view_sql %2$s NOT NULL,\n"
    + "  modified_timestamp BIGINT NOT NULL,\n"
    + "  PRIMARY KEY (view_name, view_namespace)\n"
    + ")",
    tableName,
    connector.getPayloadType()
)
```

Druid Brokers periodically refresh a snapshot of the current view state from the coordinators, and also listen for updates pushed from the Coordinator. This transfer is done using JSON Smile in both cases. Brokers then update the schema to match the current set of view definitions, making them available for querying.

### Configuration
| property | description | default |
| --- | --- | --- |
| `druid.sql.viewmanager.imply.pollingPeriod` | How frequently in milliseconds that Brokers will poll Coordinators to refresh view state | 60000 |
| `druid.sql.viewmanager.imply.maxRandomDelay` | Random delay in milliseconds between Broker polling period to prevent herd effects from overwhelming Coordinators. | 6000 |
| `druid.sql.viewmanager.imply.maxSyncRetries` | Number of retries Brokers will attempt before abandoning a view state refresh from Coordinators | 5 |
| `druid.sql.viewmanager.imply.cacheDirectory` | Local disk path which Brokers can use to store a snapshot of viewstate, to allow cold startup when coordinators are not available using previously cached view definitions. | None. |
| `druid.sql.viewmanager.imply.enableCacheNotifications` | Allow Coordinators to propagate view state changes to Brokers. | true |
| `druid.sql.viewmanager.imply.cacheNotificationTimeout` | Timeout period in milliseconds for view state change notifications. | 5000 |

## How to use it
Make some views, then query them.
### Coordinator view-manager API

The API documentation below uses the example view name `wiki-en`.

#### Create view

Request:
```
POST http://localhost:8081/druid-ext/view-manager/v1/views/wiki-en
Accept: application/json
Content-Type: application/json

{
  "viewSql": "SELECT * FROM druid.wikipedia WHERE channel = '#en'"
}
```

Response:
201 'created' status code, `Location` header
```
Location: http://localhost:8081/druid-ext/view-manager/v1/views/wiki-en
```

400 'bad request' status code if view already exists, JSON error response
```json
{
  "error": "View wiki-en already exists"
}
```

#### Alter view

Request:
```
PUT http://localhost:8081/druid-ext/view-manager/v1/views/wiki-en
Accept: application/json
Content-Type: application/json

{
  "viewSql": "SELECT * FROM druid.wikipedia WHERE channel = '#en.wikipedia'"
}
```

Response:
200 'ok' status code if view updated successfully, 404 if view does not exist

#### Delete view

Request:
```
DELETE http://localhost:8081/druid-ext/view-manager/v1/views/wiki-en
```

Response:
200 'ok' status code if view deleted successfully, 404 if view does not exist


#### List views

Request:

```
GET http://coordinator:8081/druid-ext/view-manager/v1/views
Accept: application/json
Content-Type: application/json
```

Response:
200 status code
```json
{
  "wiki-en": {
    "viewName": "wiki-en",
    "viewNamespace": null,
    "viewSql": "SELECT * FROM druid.wikipedia WHERE channel = '#en.wikipedia'",
    "lastModified": "2021-02-24T14:28:28.729Z"
  },
  "wiki-fr": {
    "viewName": "wiki-fr",
    "viewNamespace": null,
    "viewSql": "SELECT * FROM druid.wikipedia WHERE channel = '#fr.wikipedia'",
    "lastModified": "2021-02-24T15:07:29.698Z"
  }
}
```

#### Get single view definition

Request:

```
GET http://coordinator:8081/druid-ext/view-manager/v1/views/wiki-en
Accept: application/json
Content-Type: application/json
```

Response:
200 status code
```json
{
  "viewName": "wiki-en",
  "viewNamespace": null,
  "viewSql": "SELECT * FROM druid.wikipedia WHERE channel = '#en.wikipedia'",
  "lastModified": "2021-02-24T14:28:28.729Z"
}
```

#### Get single view load status

This API provides a summary of the freshness of a view definition across the cluster, for a single view.

The response contains 5 lists, containing the hostnames and ports of brokers in the cluster, organized by how fresh their
view definition is compared to the coordinator's definition at the time the request was received.

The lists are described below:
* fresh: brokers with a view definition that has the same modification time as the coordinator's view definition
* stale: brokers with an older view than the coordinator
* newer: brokers with a newer view than the coordinator (i.e. the view definition changed while the load status API was being processed)
* absent: brokers that do not have the view definition loaded
* unknown: brokers that we were unable to determine view definition version for, e.g. due to network errors when communicating with the broker

Request:

```
GET http://coordinator:8081/druid-ext/view-manager/v1/views/loadstatus/wiki-en
Accept: application/json
Content-Type: application/json
```

Response:
200 status code
```json
{
  "fresh": [
    "broker01:8082"
  ],
  "stale": [
    "broker02:8082"
  ],
  "newer": [
    "broker03:8082"
  ],
  "absent": [
    "broker04:8082"
  ],
  "unknown": [
    "broker05:8082"
  ]
}
```


#### API permissions

Accessing these APIs requires `READ` and `WRITE` access to the `CONFIG` resource type with name `CONFIG`.

Querying a view requires `READ` access to the `VIEW` resource type with the name of the view.
