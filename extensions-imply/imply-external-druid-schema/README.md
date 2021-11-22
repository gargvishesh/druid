<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Imply External Druid Schemas

This extension provides an implementation of `DruidSchemaManager`, which polls an external HTTP endpoint for the 
table schemas that should be used in the broker's SQL planner.

Add `imply-external-druid-schema` to the extensions load list, and set `druid.sql.schemamanager.type` to `imply`.

```json
druid.extensions.loadList=["druid-lookups-cached-global","druid-datasketches","imply-external-druid-schema"]
...

druid.sql.schemamanager.type=imply
...
```

## How it works

The Druid coordinator periodically polls an HTTP endpoint specified in configuration for a `Map<String, TableSchema`, where the key is the table name and the value is the schema for the table.

This map of schemas is used as the set of Druid datasource schemas in `DruidSchema`.

Druid Brokers periodically refresh a snapshot of the current table schema state from the coordinators, and also listen for updates pushed from the Coordinator. This transfer is done using JSON in both cases. The `DruidSchema` uses these table schemas instead of polling for segment metadata across the cluster, making these tables available for querying.

The HTTP client used to communicate with the HTTP endpoint is the same as the Escalated client used for internal Druid->Druid communications. 

This extension is intended for use in the Imply SaaS environment, where the SaaS tables service provides the table schema endpoint, and all services use Keycloak-based authentication.

### Configuration
| property | description | default |
| --- | --- | --- |
| `druid.sql.schemamanager.imply.pollingPeriod` | How frequently in milliseconds that Brokers will poll Coordinators to refresh schema state | 60000 |
| `druid.sql.schemamanager.imply.maxRandomDelay` | Random delay in milliseconds between Broker polling period to prevent herd effects from overwhelming Coordinators. | 6000 |
| `druid.sql.schemamanager.imply.maxSyncRetries` | Number of retries Brokers will attempt before abandoning a schema state refresh from Coordinators | 5 |
| `druid.sql.schemamanager.imply.cacheDirectory` | Local disk path which Brokers can use to store a snapshot of schema state, to allow cold startup when coordinators are not available using previously cached schema definitions. | None. |
| `druid.sql.schemamanager.imply.enableCacheNotifications` | Allow Coordinators to push schema changes to Brokers. | true |
| `druid.sql.schemamanager.imply.cacheNotificationTimeout` | Timeout period in milliseconds for schema  change notifications. | 5000 |
| `druid.sql.schemamanager.imply.notifierUpdatePeriod`| Maximum frequency at which the Coordinator will notify other node types of state updates. | 6000 |
| `druid.sql.schemamanager.imply.tablesServiceUrl` | The URL to poll for table schema definitions. | required property |
