<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~  of Imply Data, Inc.
  -->

# clarity-emitter

Druid emitter that sends events either via HTTP POST to
[clarity-collector](https://github.com/implydata/clarity-collector), or to a Kafka broker.

## Clarity HTTP

To bind, add "clarity-emitter" to your extensions list, and set `druid.emitter=clarity` in common.runtime.properties.

All the keys below should be prefixed by `druid.emitter.clarity.`, i.e. `druid.emitter.clarity.recipientBaseUrl`:

|Field|Type|Description|Default|Required|
|-----|----|-----------|-------|--------|
|`recipientBaseUrl`|String|HTTP endpoint events will be posted to, e.g. `http://<clarity collector host>:<port>/d/<username>`|[required]|yes|
|`basicAuthentication`|String|Basic auth credentials, typically `<username>:<password>`|null|no|
|`clusterName`|String|Cluster name used to tag events|null|no|
|`anonymous`|Boolean|If true, hostnames are removed from events. Additionally, the `identity` and `remoteAddress` event fields, and the `implyUser` and `implyUserEmail` metric dimensions are anonymized by a salted SHA-256 hash. This is so that clarity data can distinguish multiple high-cost queries from a single user or many different users sending high-cost queries to help troubleshoot performance issues. |false|no|
|`maxBufferSize`|Integer|Maximum size of event buffer|min(250MB, 10% of heap)|no|
|`maxBatchSize`|Integer|Maximum size of HTTP event payload |5MB|no|
|`flushCount`|Integer|Number of events before a flush is triggered|500|no|
|`flushBufferPercentFull`|Integer|Percentage of buffer fill that will trigger a flush (byte-based)|25|no|
|`flushMillis`|Integer|Period between flushes if not triggered by flushCount or flushBufferPercentFull|60s|no|
|`flushTimeOut`|Integer|Flush timeout|Long.MAX_VALUE|no|
|`timeOut`|ISO8601 Period|HTTP client response timeout|PT1M|no|
|`batchingStrategy`|String [ARRAY, NEWLINES]|How events are batched together in the payload|ARRAY|no|
|`compression`|String [NONE, LZ4, GZIP]|Compression algorithm used|LZ4|no|
|`lz4BufferSize`|Integer|Block size for the LZ4 compressor in bytes|65536|no|
|`samplingRate`|Integer|For sampled metrics, what percentage of metrics will be emitted|100|no|
|`sampledMetrics`|List<String>|Which event types are sampled|`["query/wait/time", "query/segment/time", "query/segmentAndCache/time"]`|no|
|`sampledNodeTypes`|List<String>|Which node types are sampled|`["druid/historical", "druid/peon", "druid/realtime"]`|no|
|`customQueryDimensions`|List<String>|Context dimensions that will be extracted and emitted. When emitted, they will be prepended with the string `context:`.|`[]`|no|
|`metricsFactoryChoice`|Boolean|If true, registers Clarity's custom query metrics modules as options rather than defaults. Useful if you are defining your own query metrics factories through your own extensions.|false|no|
|`context`|Map<String, Object>|The keys in this map will be added as additional dimensions to all metrics. They will not override other keys if there's a collision.|null|no|

### HTTPS Context Configuration

Clarity HTTP supports HTTPS (TLS) without any special configuration. But you can specify extra configuration if you need
to use a custom trust store. All the keys below should be prefixed by `druid.emitter.clarity.ssl`,
e.g. `druid.emitter.clarity.ssl.trustStorePath`.

If `trustStorePath` is not specified, a custom SSL context will not be created, and Druid's default SSL context will be
used instead.

|Property|Description|Default|Required|
|--------|-----------|-------|--------|
|`protocol`|SSL protocol to use.|`TLSv1.2`|no|
|`trustStoreType`|The type of the key store where trusted root certificates are stored.|`java.security.KeyStore.getDefaultType()`|no|
|`trustStorePath`|The file path or URL of the TLS/SSL Key store where trusted root certificates are stored.|none|no|
|`trustStoreAlgorithm`|Algorithm to be used by TrustManager to validate certificate chains|`javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm()`|no|
|`trustStorePassword`|The [Password Provider](https://github.com/implydata/druid/blob/0.10.1-csp/docs/content/operations/password-provider.md) or String password for the Trust Store.|none|yes, if `trustStorePath` is specified.|

## Clarity Kafka

To bind, add "clarity-emitter-kafka" to your extensions list, and set `druid.emitter=clarity-kafka` in common.runtime.properties.

All the keys below should be prefixed by `druid.emitter.clarity.`, i.e. `druid.emitter.clarity.recipientBaseUrl`:

|Field|Type|Description|Default|Required|
|-----|----|-----------|-------|--------|
|`topic`|String|Kafka topic|[required]|yes|
|`producer.bootstrap.servers`|String|Kafka "bootstrap.servers" configuration (a list of brokers)|[required]|yes|
|`producer.*`|String|Can be used to specify any other Kafka producer property.|empty|no|
|`clusterName`|String|Cluster name used to tag events|null|no|
|`anonymous`|Boolean|Should hostnames be scrubbed from events?|false|no|
|`maxBufferSize`|Integer|Maximum size of event buffer|min(250MB, 10% of heap)|no|
|`samplingRate`|Integer|For sampled metrics, what percentage of metrics will be emitted|100|no|
|`sampledMetrics`|List<String>|Which event types are sampled|["query/wait/time", "query/segment/time", "query/segmentAndCache/time"]|no|
|`sampledNodeTypes`|List<String>|Which node types are sampled|["druid/historical", "druid/peon", "druid/realtime"]|no|
|`customQueryDimensions`|List<String>|Context dimensions that will be extracted and emitted. When emitted, they will be prepended with the string `context:`.|`[]`|no|
