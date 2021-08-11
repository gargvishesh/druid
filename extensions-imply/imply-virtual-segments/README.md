<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Virtual segment extension

## Introduction
Virtual segment extension enables a historical to download segments lazily at the query time, instead of pre-fetching
the segment. It also enables the historical to assign itself more segments than what can be stored on the disk. If the extension
is enabled on the historical, historical will evict segments from the disk if new segments need to be downloaded and there 
is not enough space on the disk. This allows users to query far more data than what can be fit on the disk.

## Using the extension
The extension is not supposed to be used directly but via cloud manager interface for cold-tier. Cloud manager exposes
a toggle switch to enable cold-tier. The historicals belonging to cold-tier will have this extension loaded 
out-of-the-box. The extension can be loaded on any service but before use, it must be enabled 
explicitly (Refer to [configuration](#configuration)). The extension is disabled by default. 

It must not be enabled on non-historical services. 

## Configuration

| property | description | default |
| --- | --- | --- |
| `druid.virtualSegment.enabled` | Whether the extension should be enabled or not. This value should be set to `true` on cold historicals when using cold-tier. | false |
| `druid.virtualSegment.downloadThreads` | Number of threads downloading the segments. |  `Runtime.getRuntime().availableProcessors()`  |
| `druid.virtualSegment.downloadDelayMs` | Time interval in milliseconds between successive download requests if no space was available. | 10 |

# Cold-tier
Virtual segment extension by itself is not usable but it is a key piece for enabling cold-tier. For the cold-tier to 
work, any historical that is part of cold-tier, must be loaded with this extension. Enabling this extension will allow
de-coupling storage and compute for cold data. Other brokers and historicals need to be configured as well
for query isolation between hot and cold tier. The relevant configuration is described below

## Cold-tier historical configuration
To enable cold-tier on historicals with lazy segment loading, the extension has to be loaded and following configuration
needs to be set

```
druid.virtualSegment.enabled=true
druid.server.tier=_cold_tier
```
The tier name is set to `_cold_tier` that distinguishes it from the hot tier. 

## Hot-tier historical configuration
No configuration change is required on hot historicals.

## Cold-tier broker configuration

```
druid.service=druid/broker-cold
druid.broker.segment.watchedTiers=["_cold_tier"]

```

## Hot-tier broker configuration

```
druid.broker.segment.watchedTiers=["_default_tier"]
```

Above configuration ensures that hot brokers are only aware of the segments that are on hot historicals. If you have
more hot historical tiers, they should be added to the list in above configuration.

## Router configuration


```
druid.router.strategies=[{"type":"manual","defaultManualBrokerService":"druid/broker"}]
druid.router.tierToBrokerMap={"_cold_tier":"druid/broker-cold", "_default_tier":"druid/broker"}
```

## Loading data on cold-tier

To load data on cold-tier, add a new load rule via web-console. For example, add the following load rules to keep 
the last 6 months of data on cold-tier and 1 month of data on hot-tier.

Rule-1 - This rule instructs druid to keep 1 replica in hot-tier and 1 replica in cold-tier for any segment that falls 
in the last 1 month interval. 

```json
{
  "type" : "loadByPeriod",
  "period" : "P1M",
  "includeFuture" : true,
  "tieredReplicants": {
      "_cold_tier": 1,
      "_default_tier" : 1
  }
}
```

Rule-2 - This rule instructs druid to keep 1 replica in cold-tier for any segment that falls in the last 6 month interval. Rule-2
will be evaulated only for those segments that do not match Rule-1.
```json
{
  "type" : "loadByPeriod",
  "period" : "P6M",
  "includeFuture" : true,
  "tieredReplicants": {
      "_cold_tier": 1
  }
}
```

Rule-1 must appear before Rule-2 when reading the rules from the top in console. This ensures that for same segment, 
both rules will not be applied together.

## Querying data on cold-tier

To query the data from cold-tier, caller must set the `brokerService` to `druid/broker-cold` in the query context. Only queries
with correct value of `brokerService` will go to cold-tier. If the value is not set or incorrect, the queries will go to hot-tier and query 
only those segments that are loaded on hot historicals. 
