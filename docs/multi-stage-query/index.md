---
id: index
title: Multi-stage query engine
---

> The multi-stage query engine is an alpha feature available in Imply 2022.01 and later. All
> functionality documented on this page is subject to change or removal at any time. Alpha features
> are provided "as is" and are not subject to Imply SLAs.

This extension provides a new, multi-stage distributed query engine for Apache Druid. It supports
larger, heavier-weight queries, querying external data, and ingestion via SQL **INSERT**. Read our
blog post for details about why we are creating this engine:
https://imply.io/blog/a-new-shape-for-apache-druid/

The multi-stage query engine hooks into the existing data processing routines from Druid's core
query engine, so it has all the same query capabilities. But on top of that, it adds a system that
splits queries into stages and automatically exchanges data between stages. Each stage is
parallelized to run across many data servers at once. There isn't any need for tuning beyond
selecting a concurrency level. This means that the multi-stage engine excels at executing queries
that would be bottlenecked at the Broker when using Druid's core query engine.

With multi-stage queries, Druid can:

- Read external data at query time using the [`EXTERN`](#extern) function.
- Execute batch ingestion jobs as SQL queries using the [`INSERT`](#insert) keyword.
- Transform and rewrite existing tables using SQL queries.
- Execute heavy-weight queries that return large numbers of rows and may run for a long time.
- Execute queries that need to exchange large amounts of data between servers, like exact count
  distinct of high-cardinality fields.

The engine is code-named "Talaria". During the alpha, you'll see this term in certain API calls and
in the name of the extension. The name comes from Mercury’s winged sandals, representing the
exchange of data between servers.

## Setup

To try out the multi-stage query engine in Imply Enterprise Hybrid, launch a cluster with the following settings.

**Imply version**

We recommend using the latest STS release. The minimum version is 2022.01 STS.

Note: the multi-stage query engine is only available for STS versions of Imply at this time. It is
not available in 2022.01 LTS.

**Custom extensions**

Add two entries:

1. Name `imply-talaria`, and "S3 path or url" blank.
2. Name `imply-sql-async`, and "S3 path or url" blank.

**Feature flags**

- Enable *HTTP-based task management.* This is recommended in all cases.
- Disable *Use Indexers instead of Middle Managers.* Middle Managers are recommended for all PoC and
  production workloads.

> Indexers have lower task launching overhead than Middle Managers, so you may prefer to enable *Use
> Indexers instead of Middle Managers* in demo environments where it is important that a series of
> fast-executing queries completes quickly. However, at this time, Middle Managers are more
> battle-tested and are therefore recommended for PoCs and production.
>
> Once all worker tasks are launched, throughput is similar between Indexers and Middle Managers.

**Common**

In the Common property overrides, add an Imply license string that includes
the `talaria` feature, and configure the `imply-sql-async` extension.

```
imply.license=<license string>

# Configure imply-sql-async extension.
# Note: this setup only works for multi-stage query; for regular async query you must use storage type "s3".
druid.query.async.storage.type=local
druid.query.async.storage.local.directory=/mnt/tmp/async-query
```

**Indexer** or **Middle Manager**

```bash
druid.server.http.numThreads=1025
druid.indexer.fork.property.druid.server.http.numThreads=1025
druid.global.http.numConnections=50
druid.global.http.eagerInitialization=false

# Required for Java 11
jvm.config.dsmmap=--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
druid.indexer.runner.javaOptsArray=["--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"]
```

**Broker**

```bash
druid.sql.executor.type=imply
```

## Quickstart

Before running through this quickstart, run through [Setup](#setup) to get a cluster started. Once
you've done that, the best way to start running multi-stage queries is with the enhanced query view
in the Druid web console.

To get started with this view:

1. Select **Manage data** in Imply Enterprise Hybrid to open the Druid console.
2. Select **Query** from the top menu.
3. Click the dots "..." next to *Run* and select **talaria** from the **Query engine** menu. If you
don't see this menu, try alt- or option-clicking the Druid logo first.

Notice that the SQL editing field appears in a tab. If you like, you can
open multiple tabs to run queries simultaneously.

![Empty UI](../assets/multi-stage-query/ui-empty.png)

Run the following query to try the multi-stage query engine with external sample data.

```sql
SELECT *
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
LIMIT 100
```

The multi-stage query engine allows querying external data with the [`EXTERN`](#extern) function.

To ingest the query results into a table, run the following query.

```sql
INSERT INTO wikipedia
SELECT
  TIME_PARSE("timestamp") AS __time,
  *
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

The `PARTITIONED BY DAY` clause specifies day-based segment granularity. The
data is inserted into the `wikipedia` table.

The data is now queryable. Run the following query to analyze the data
in the ingested table, producing a list of top channels.

```sql
SELECT
  channel,
  COUNT(*)
FROM wikipedia
GROUP BY channel
ORDER BY COUNT(*) DESC
```

The same query would also work on the external data directly.

```sql
SELECT
  channel,
  COUNT(*)
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
GROUP BY channel
ORDER BY COUNT(*) DESC
```

For more examples, see the [Example queries](#example-queries) section.

## Key concepts

The multi-stage query engine extends Druid's query stack to handle
asynchronous queries that can exchange data between stages.

At this time, queries execute using indexing service tasks, even for
regular SELECT (non-INSERT) queries. At least two task slots per query
are occupied while a query is running. This behavior is subject to
change in future releases.

Key concepts for multi-stage query execution:

- **Controller**: an indexing service task of type `talaria0` that manages
  the execution of a query. There is one controller task per query.

- **Worker**: indexing service tasks of type `talaria1` that execute a
  query. There may be more than one worker task per query. Internally,
  the tasks process items in parallel using their processing pools.
  (i.e., up to `druid.processing.numThreads` of execution parallelism
  within a worker task).

- **Stage**: a stage of query execution that is parallelized across
  worker tasks. Workers exchange data with each other between stages.

- **Partition**: a slice of data output by worker tasks. In INSERT
  queries, the partitions of the final stage become Druid segments.

- **Shuffle**: workers exchange data between themselves on a per-partition basis in a process called
  "shuffling". During a shuffle, each output partition is sorted by a clustering key.

When you use the multi-stage query engine, the following happens:

1.  The **Broker** plans your SQL query into a native query, as usual.

2.  The Broker wraps the native query into a task of type `talaria0`
    and submits it to the indexing service. The Broker returns the task
    ID to you and exits.

3.  The **controller task** launches a wave of **worker tasks** based on
    the `talariaNumTasks` query context parameter.

4.  The worker tasks execute the query.

5.  If the query is a SELECT query, the worker tasks send the results
    back to the controller task, which writes them into its task report.
    If the query is an INSERT query, the worker tasks generate and
    publish new Druid segments to the provided datasource.

6.  For multi-stage queries, async query APIs such as [status](#get-query-status),
    [results](#get-query-results), and [details](#get-query-details) work by
    fetching task status and task reports under the hood.

## Query API

### Submit a query

> Multi-stage query APIs are in a work-in-progress state for the alpha. They may change in future
> releases. For stability reasons, we recommend using the [web console](#web-console) if you do not
> need a programmatic interface.

#### Request

Submit queries using the [`POST /druid/v2/sql/async/`](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#submit-a-query) API provided by
the `imply-sql-async` extension, with `talaria: true` set as a
[context parameter](https://docs.imply.io/latest/druid/querying/sql.html#connection-context).

Currently, the multi-stage query engine ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. It always behaves as if `resultFormat` is "array", `header` is
true, `typesHeader` is true, and `sqlTypesHeader` is true.

See below for an example request and response. For more details, refer to the
[`imply-sql-async` documentation](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#submit-a-query).

**HTTP**

```
POST /druid/v2/sql/async/
```

```json
{
  "query" : "SELECT 1 + 1",
  "context" : {
    "talaria" : true
  }
}
```

**curl**

```
curl -XPOST -H'Content-Type: application/json' \
  https://IMPLY-ENDPOINT/druid/v2/sql/async/ \
  -u 'user:password' \
  --data-binary '{"query":"SELECT 1 + 1","context":{"talaria":true}}'
```

**Query syntax**

The `query` can be any [Druid SQL](https://docs.imply.io/latest/druid/querying/sql.html) query,
subject to the limitations mentioned in the [Known issues](#known-issues) section. In addition to
standard Druid SQL syntax, queries that run with the multi-stage query engine can use two additional
features:

- [INSERT INTO ... SELECT](#insert) to insert query results into Druid datasources.

- [EXTERN](#extern) to query external data from S3, GCS, HDFS, and so on.

**Context parameters** <a href="#context" name="context">#</a>

The multi-stage query engine accepts additional Druid SQL
[context parameters](https://docs.imply.io/latest/druid/querying/sql.html#connection-context).

> Context comments like `--:context talariaFinalizeAggregations: false`
> are available only in the web console. When using the API, context comments
> will be ignored, and context parameters must be provided in the `context`
> section of the JSON object.

|Parameter|Description|Default value|
|----|-----------|----|
| talaria| True to execute using the multi-stage query engine; false to execute using Druid's native query engine| false|
| talariaNumTasks| (SELECT or INSERT)<br /><br />The multi-stage query engine executes queries using the indexing service, i.e. using the Overlord + MiddleManager / Indexer. This property specifies the number of worker tasks to launch.<br /><br />The total number of tasks launched will be `talariaNumTasks` + 1, because there will also be a controller task.<br /><br />All tasks must be able to launch simultaneously. If they cannot, the query will not be able to execute. Therefore, it is important to set this parameter at most one lower than the total number of task slots.| 1 |
| talariaFinalizeAggregations | (SELECT or INSERT)<br /><br />Whether Druid will finalize the results of complex aggregations that directly appear in query results.<br /><br />If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. | true |
| talariaReplaceTimeChunks | (INSERT only)<br /><br />Whether Druid will replace existing data in certain time chunks during ingestion. This can either be the word "all" or a comma-separated list of intervals in ISO8601 format, like `2000-01-01/P1D,2001-02-01/P1D`. The provided intervals must be aligned with the granularity given in `PARTITIONED BY` clause.<br /><br />At the end of a successful query, any data previously existing in the provided intervals will be replaced by data from the query. If the query generates no data for a particular time chunk in the list, then that time chunk will become empty. If set to `all`, the results of the query will replace all existing data.<br /><br />All ingested data must fall within the provided time chunks. If any ingested data falls outside the provided time chunks, the query will fail with an [InsertTimeOutOfBounds](#errors) error.<br /><br />When `talariaReplaceTimeChunks` is set, all `CLUSTERED BY` columns are singly-valued strings, and there is no LIMIT or OFFSET, then Druid will generate "range" shard specs. Otherwise, Druid will generate "numbered" shard specs. | null<br /><br />(i.e., append to existing data, rather than replace)|
| talariaRowsInMemory | (INSERT only)<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries.<br /><br />In most cases, you should stick to the default. It may be necessary to override this if you run into one of the current [known issues around memory usage](#known-issues-memory)</a>.

#### Response

```json
{
  "asyncResultId": "talaria-sql-58b05e02-8f09-4a7e-bccd-ea2cc107d0eb",
  "state": "RUNNING",
  "resultFormat": "array",
  "engine": "Talaria-Indexer"
}
```

**Response fields**

|Field|Description|
|-----|-----------|
|asyncResultId|Controller task ID.<br /><br />Druid's standard [task APIs](https://docs.imply.io/latest/druid/operations/api-reference.html#overlord) can be used to interact with this controller task.|
|state|Initial state for the query, which is "RUNNING".|
|resultFormat|Always "array", regardless of what was specified in your original query, because the multi-stage engine currently only supports the "array" result format.|
|engine|String "Talaria-Indexer" if this query was run with the multi-stage engine.|

### Get query status

> Multi-stage query APIs are in a work-in-progress state for the alpha. They may change in future
> releases. For stability reasons, we recommend using the [web console](#web-console) if you do not
> need a programmatic interface.

Poll the [`GET /druid/v2/sql/async/{asyncResultId}/status`](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#get-query-status) API
provided by the `imply-sql-async` extension.

See below for an example request and response. For more details, refer to the
[`imply-sql-async` documentation](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#get-query-status).

#### Request

```
GET /druid/v2/sql/async/{asyncResultId}/status
```

**curl**

```
curl -u 'user:password' \
  https://IMPLY-ENDPOINT/druid/v2/sql/async/ASYNC-RESULT-ID/status
```

#### Response

```json
{
  "asyncResultId": "talaria-sql-bc9aa8b8-05fc-4223-ad07-3edfb427c709",
  "state": "COMPLETE",
  "resultFormat": "array",
  "engine": "Talaria-Indexer"
}
```

### Get query results

> Multi-stage query APIs are in a work-in-progress state for the alpha. They may change in future
> releases. For stability reasons, we recommend using the [web console](#web-console) if you do not
> need a programmatic interface.

If the query succeeds, and is a `SELECT` query, obtain the results using the
[`GET /druid/v2/sql/async/{asyncResultId}/results`](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#get-query-results) API
provided by the `imply-sql-async` extension.

See below for an example request and response. For more details, refer to the
[`imply-sql-async` documentation](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#get-query-results).

#### Request

```
GET /druid/v2/sql/async/{asyncResultId}/results
```

**curl**

```
curl -u 'user:password' \
  https://IMPLY-ENDPOINT/druid/v2/sql/async/ASYNC-RESULT-ID/results
```

#### Response

```json
[["EXPR$0"],["LONG"],["INTEGER"],[2]]
```

Currently, the multi-stage query engine ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. It always behaves as if `resultFormat` is "array", `header` is
true, `typesHeader` is true, and `sqlTypesHeader` is true.

### Cancel a query

> Multi-stage query APIs are in a work-in-progress state for the alpha. They may change in future
> releases. For stability reasons, we recommend using the [web console](#web-console) if you do not
> need a programmatic interface.

Cancel a query using the
[`DELETE /druid/v2/sql/async/{asyncResultId}`](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#cancel-a-query) API
provided by the `imply-sql-async` extension.

When a multi-stage query is canceled, the query results and metadata are not removed. This differs
from Broker-based async queries, where query results and metadata are removed when a query is
canceled.

See below for an example request. For more details, refer to the
[`imply-sql-async` documentation](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#cancel-a-query).

### Get query details

> Multi-stage query APIs are in a work-in-progress state for the alpha. They may change in future
> releases. For stability reasons, we recommend using the [web console](#web-console) if you do not
> need a programmatic interface.

Detailed information about query execution is available for queries run with the multi-stage engine,
including:

- Status (running, successful exit, failure).
- Stages of query execution, along with their current progress.
- Any errors encountered.

Query details are physically stored in the task report for the controller
task.

The details API may return `500 Server Error` or `404 Not Found` when
tasks are in the process of starting up or shutting down. If you encounter
this error for a task that you expect should have details available, either
try again later or check the [Status API](#get-query-status) to see if the
task has failed before it was able to write its report.

It is possible for the query status in the details API to differ from the
query status in the [Status API](#get-query-status). This can happen because
the details are sourced from the task report, which is written by the
controller task itself, but the final status is sourced from Druid's task
execution framework. For example, it is possible for the details to say
`SUCCESS`, but the actual task to say `FAILED`, if there is a failure in
cleanup after the report has been written.

#### Request

**HTTP**

```
GET /druid/v2/sql/async/{asyncResultId}
```

**curl**

```
curl -u 'user:password' \
  https://IMPLY-ENDPOINT/druid/v2/sql/async/ASYNC-RESULT-ID
```

#### Response

**Report for successful query**

```json
{
  "talaria": {
    "taskId": "talaria-sql-1d723aea-e774-48ba-a2cc-b9cfdd77e3ce",
    "payload": {
      "status": {
        "status": "SUCCESS",
        "startTime": "2022-03-10T01:41:58.105Z",
        "durationMs": 1608
      },
      "stages": [
        {
          "stageNumber": 0,
          "inputStages": [],
          "processorType": "GroupByPreShuffleFrameProcessorFactory",
          "phase": "RESULTS_COMPLETE",
          "workerCount": 2,
          "partitionCount": 2,
          "inputFileCount": 1,
          "startTime": "2022-03-10T01:41:58.446Z",
          "duration": 1085,
          "clusterBy": {
            "columns": [
              {
                "columnName": "d0"
              }
            ]
          },
          "query": { ... }
        },
        {
          "stageNumber": 1,
          "inputStages": [0],
          "processorType": "GroupByPostShuffleFrameProcessorFactory",
          "phase": "RESULTS_COMPLETE",
          "workerCount": 2,
          "partitionCount": 2,
          "startTime": "2022-03-10T01:41:59.513Z",
          "duration": 74
        },
        {
          "stageNumber": 2,
          "inputStages": [1],
          "processorType": "OrderByFrameProcessorFactory",
          "phase": "RESULTS_COMPLETE",
          "workerCount": 2,
          "partitionCount": 1,
          "startTime": "2022-03-10T01:41:59.573Z",
          "duration": 140,
          "clusterBy": {
            "columns": [
              {
                "columnName": "a0",
                "descending": true
              }
            ]
          }
        }
      ]
    },
    "task": { ... }
  }
}
```

**Report for failed query**

```json
{
  "talaria": {
    "taskId": "talaria-sql-wikipedia-df963a1e-caa4-4a6f-9494-1f72395eda9f",
    "payload": {
      "status": {
        "status": "FAILED",
        "startTime": "2022-03-10T01:51:07.698Z",
        "durationMs": 2391,
        "errorReport": {
          "taskId": "talaria1_wikipedia_gbjlnmph_2022-03-10T01:51:07.698Z",
          "host": "localhost:8091",
          "stageNumber": 0,
          "error": {
            "errorCode": "TooManyBuckets",
            "maxBuckets": 5000,
            "errorMessage": "Too many partition buckets (max = 5,000); try breaking your query up into smaller queries or using a wider segmentGranularity"
          },
          "exceptionStackTrace": "io.imply.druid.talaria.frame.cluster.statistics.TooManyBucketsException: Too many buckets; maximum is [5000]\n\tat io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl.getOrCreateBucketHolder(ClusterByStatisticsCollectorImpl.java:339)\n\tat io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl.add(ClusterByStatisticsCollectorImpl.java:111)\n\tat io.imply.druid.talaria.indexing.TalariaClusterByStatisticsCollectionProcessor.runIncrementally(TalariaClusterByStatisticsCollectionProcessor.java:110)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessors$1FrameProcessorWithBaggage.runIncrementally(FrameProcessors.java:136)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.runProcessorNow(FrameProcessorExecutor.java:197)\n\tat io.imply.druid.talaria.frame.processor.FrameProcessorExecutor$1ExecutorRunnable.run(FrameProcessorExecutor.java:112)\n\tat io.imply.druid.talaria.exec.WorkerImpl$2$2.run(WorkerImpl.java:589)\n\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n\tat java.base/java.util.concurrent.FutureTask.run$$$capture(FutureTask.java:264)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java)\n\tat org.apache.druid.query.PrioritizedListenableFutureTask.run(PrioritizedExecutorService.java:251)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\n"
        }
      },
      "stages": [ ... ]
    },
    "task": { ... }
  }
}
```

**Response fields**

|Field|Description|
|-----|-----------|
|talaria.taskId|Controller task ID.|
|talaria.payload.status|Query status container.|
|talaria.payload.status.status|RUNNING, SUCCESS, or FAILED.|
|talaria.payload.status.startTime|Start time of the query in ISO format. Only present if the query has started running.|
|talaria.payload.status.durationMs|Milliseconds elapsed after the query has started running. -1 denotes that the query hasn't started running yet.|
|talaria.payload.status.errorReport|Error object. Only present if there was an error.|
|talaria.payload.status.errorReport.taskId|The task that reported the error, if known. May be a controller task or a worker task.|
|talaria.payload.status.errorReport.host|The hostname and port of the task that reported the error, if known.|
|talaria.payload.status.errorReport.stageNumber|The stage number that reported the error, if it happened during execution of a specific stage.|
|talaria.payload.status.errorReport.error|Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](#errors). Always present if there is an error.|
|talaria.payload.status.errorReport.error.errorCode|One of the error codes from the [error code table](#errors). Always present if there is an error.|
|talaria.payload.status.errorReport.error.errorMessage|User-friendly error message. Not always present, even if there is an error.|
|talaria.payload.status.errorReport.exceptionStackTrace|Java stack trace in string form, if the error was due to a server-side exception.|
|talaria.payload.stages|Array of query stages.|
|talaria.payload.stages[].stageNumber|Each stage has a number that differentiates it from other stages.|
|talaria.payload.stages[].inputStages|Array of input stage numbers.|
|talaria.payload.stages[].stageType|String that describes the logic of this stage. This is not necessarily unique across stages.|
|talaria.payload.stages[].phase|Either NEW, READING_INPUT, POST_READING, RESULTS_COMPLETE, or FAILED. Only present if the stage has started.|
|talaria.payload.stages[].workerCount|Number of parallel tasks that this stage is running on. Only present if the stage has started.|
|talaria.payload.stages[].partitionCount|Number of output partitions generated by this stage. Only present if the stage has started and has computed its number of output partitions.|
|talaria.payload.stages[].inputFileCount|Number of external input files or Druid segments read by this stage. Does not include inputs from other stages in the same query.|
|talaria.payload.stages[].startTime|Start time of this stage. Only present if the stage has started.|
|talaria.payload.stages[].query|Native Druid query for this stage. Only present for the first stage that corresponds to a particular native Druid query.|
|talaria.task|Controller task payload.|

**Error codes** <a href="#errors" name="errors">#</a>

Possible values for `talariaStatus.payload.errorReport.error.errorCode` are:

|Code|Meaning|Additional fields|
|----|-----------|----|
|  BroadcastTablesTooLarge  | Size of the broadcast tables used in right hand side of the joins exceeded the memory reserved for them in a worker task  | &bull;&nbsp;maxBroadcastTablesSize: Memory reserved for the broadcast tables, measured in bytes |
|  Canceled  |  The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>|    |
|  CannotParseExternalData  |  A worker task could not parse data from an external data source.  |    |
|  ColumnTypeNotSupported  |  The query tried to use a column type that is not supported by the frame format.<br /><br />This currently occurs with ARRAY types, which are not yet implemented for frames.  | &bull;&nbsp;columnName<br /> <br />&bull;&nbsp;columnType   |
|  InsertCannotAllocateSegment  |  The controller task could not allocate a new segment ID due to conflict with pre-existing segments or pending segments. Two common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>|   &bull;&nbsp;dataSource<br /> <br />&bull;&nbsp;interval: interval for the attempted new segment allocation  |
|  InsertCannotBeEmpty  |  An INSERT query did not generate any output rows, in a situation where output rows are required for success.<br /> <br />Can happen for INSERT queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`.  |  &bull;&nbsp;dataSource  |
|  InsertCannotOrderByDescending  |  An INSERT query contained an `CLUSTERED BY` expression with descending order.<br /> <br />Currently, Druid's segment generation code only supports ascending order.  |   &bull;&nbsp;columnName |
|  InsertCannotReplaceExistingSegment  |  An INSERT query, with `talariaReplaceTimeChunks` set, cannot proceed because an existing segment partially overlaps those bounds and the portion within those bounds is not fully overshadowed by query results. <br /> <br />There are two ways to address this without modifying your query:<ul><li>Shrink `talariaReplaceTimeChunks` to match the query results.</li><li>Expand talariaReplaceTimeChunks to fully contain the existing segment.</li></ul>| &bull;&nbsp;segmentId: the existing segment  |
|  InsertTimeOutOfBounds  |  An INSERT query generated a timestamp outside the bounds of the `talariaReplaceTimeChunks` parameter.<br /> <br />To avoid this error, consider adding a WHERE filter to only select times from the chunks that you want to replace.  |  &bull;&nbsp;interval: time chunk interval corresponding to the out-of-bounds timestamp  |
| QueryNotSupported   |   QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that the multi-stage engine does not yet support, like GROUPING SETS. |    |
|  RowTooLarge  |  The query tried to process a row that was too large to write to a single frame.<br /> <br />See the Limits table for the specific limit on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing.  |   &bull;&nbsp;maxFrameSize: the limit on frame size which was exceeded |
|  TooManyBuckets  |  Too many partition buckets for a stage.<br /> <br />Currently, partition buckets are only used for segmentGranularity during INSERT queries. The most common reason for this error is that your segmentGranularity is too narrow relative to the data. See the [Limits](#limits) table for the specific limit.  |  &bull;&nbsp;maxBuckets: the limit on buckets which was exceeded  |
| TooManyInputFiles | Too many input files/segments per worker.<br /> <br />See the [Limits](#limits) table for the specific limit. |&bull;&nbsp;numInputFiles: the total number of input files/segments for the stage<br /><br />&bull;&nbsp;maxInputFiles: the maximum number of input files/segments per worker per stage<br /><br />&bull;&nbsp;minNumWorker: the minimum number of workers required for a sucessfull run |
|  TooManyPartitions   |  Too many partitions for a stage.<br /> <br />The most common reason for this is that the final stage of an INSERT query generated too many segments. See the [Limits](#limits) table for the specific limit.  | &bull;&nbsp;maxPartitions: the limit on partitions which was exceeded    |
|  TooManyColumns |  Too many output columns for a stage.<br /> <br />See the [Limits](#limits) table for the specific limit.  | &bull;&nbsp;maxColumns: the limit on columns which was exceeded   |
|  TooManyWorkers |  Too many workers running simultaneously.<br /> <br />See the [Limits](#limits) table for the specific limit.  | &bull;&nbsp;workers: a number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage, if multiple stages are running simultaneously. <br /><br />&bull;&nbsp;maxWorkers: the hard or soft limit on workers which was exceeded   |
|  NotEnoughMemory  |  Not enough memory to launch a stage.  |  &bull;&nbsp;serverMemory: the amount of memory available to a single process<br /><br />&bull;&nbsp;serverWorkers: the number of workers running in a single process<br /><br />&bull;&nbsp;serverThreads: the number of threads in a single process  |
|  WorkerFailed  |  A worker task failed unexpectedly.  |  &bull;&nbsp;workerTaskId: the id of the worker task  |
|  UnknownError   |  All other errors.  |    |

#### Request

```
DELETE /druid/v2/sql/async/{asyncResultId}
```

**curl**

```
curl -XDELETE -u 'user:password' \
  https://IMPLY-ENDPOINT/druid/v2/sql/async/ASYNC-RESULT-ID
```

## EXTERN

> EXTERN syntax is in a work-in-progress state for the alpha. It is expected to
> change in future releases.

Queries run with the multi-stage engine can access external data through the `EXTERN` table
function. It can appear in the form `TABLE(EXTERN(...))` anywhere a table is expected.

For example:

```sql
SELECT
  *
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
LIMIT 100
```

The `EXTERN` function takes three parameters:

1.  Any [Druid input
source](https://docs.imply.io/latest/druid/ingestion/native-batch.html#input-sources),
    as a JSON-encoded string.

2.  Any [Druid input
    format](https://docs.imply.io/latest/druid/ingestion/data-formats.html#input-format),
    as a JSON-encoded string.

3.  A row signature, as a JSON-encoded array of column descriptors. Each
    column descriptor must have a `name` and a `type`. The type can be
    `string`, `long`, `double`, or `float`. This row signature will be
    used to map the external data into the SQL layer.

External data can be used with either SELECT or INSERT queries. The
following query illustrates joining external data with other external
data.

## INSERT

### INSERT INTO ... SELECT

> INSERT syntax is in a work-in-progress state for the alpha. It is expected to
> change in future releases.

With the multi-stage query engine, Druid can use the results of a query to create a new datasource
or to append or replace data in an existing datasource. In Druid, these operations are all performed
using `INSERT INTO ... SELECT` syntax.

For example:

```sql
INSERT INTO w000
SELECT
  TIME_PARSE("timestamp") AS __time,
  *
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

To run an INSERT query:

1. Activate the multi-stage engine by setting `talaria: true` as a [context parameter](#context).
2. Add `INSERT INTO <dataSource>` at the start of your query.
3. Add a [`PARTITIONED BY`](#partitioned-by) clause to your INSERT statement. For example, use `PARTITIONED BY DAY` for daily partitioning, or `PARTITIONED BY ALL TIME` to skip time partitioning completely.
4. Optionally, add a [`CLUSTERED BY`](#clustered-by) clause.
5. Optionally, to replace data in an existing datasource, set the [`talariaReplaceTimeChunks`](#context) context parameter.

There is no syntactic difference between creating a new datasource and
inserting into an existing datasource. The `INSERT INTO <dataSource>` command
works even if the datasource does not yet exist.

All SELECT functionality is available for INSERT queries. However, keep in mind that the multi-stage
query engine does not yet implement all native Druid query features. See
[Known issues](#known-issues) for a list of functionality that is not yet implemented.

For examples, refer to the [Example queries](#example-queries) section.

### Primary timestamp

Druid tables always include a primary timestamp named `__time`.

Typically, Druid tables also use time-based partitioning, such as
`PARTITIONED BY DAY`. In this case your INSERT query must include a column
named `__time`. This will become the primary timestamp and will be used for
partitioning.

When you use `PARTITIONED BY ALL` or `PARTITIONED BY ALL TIME`, time-based
partitioning is disabled. In this case your INSERT query does not need
to include a `__time` column. However, a `__time` column will still be created
in your Druid table with all timestamps set to 1970-01-01 00:00:00.

### PARTITIONED BY

Time-based partitioning is determined by the PARTITIONED BY clause.
This clause is required for INSERT queries. Possible arguments include:

- Time unit: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is an ISO8601 period like 'PT1H'. The first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where TimeUnit is any unit supported by the [FLOOR function](https://druid.apache.org/docs/latest/querying/sql.html#time-functions). The first argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single time chunk.

If `PARTITIONED BY` is set to anything other than `ALL` or `ALL TIME`, you cannot use LIMIT or OFFSET at the outer level of your query.

### CLUSTERED BY

Secondary partitioning within a time chunk is determined by the `CLUSTERED BY`
clause. `CLUSTERED BY` can partition by any number of columns or expressions.

It is OK to mix time partitioning with secondary partitioning. For example, you can
combine `PARTITIONED BY HOUR` with `CLUSTERED BY channel` to perform
time partitioning by hour and secondary partitioning by channel within each hour.

INSERT INTO … SELECT queries cannot include `ORDER BY` as part of the SELECT.
Similar functionality can be achieved by using `CLUSTERED BY` instead.

### Rollup

Rollup is determined by the query's GROUP BY clause. The expressions in the GROUP BY clause become
dimensions, and aggregation functions become metrics.

#### Ingest-time aggregations

When performing rollup using aggregations, it is important to use aggregators
that return _nonfinalized_ state. This allows you to perform further rollups
at query time. To achieve this, set `talariaFinalizeAggregations: false` in your
INSERT query context and refer to the following table for any additional
modifications needed.

Check out the [INSERT with rollup example query](#example-insert-rollup) to see this feature in
action.

|Query-time aggregation|Notes|
|----------------------|-----|
|SUM|Use unchanged at ingest time.|
|MIN|Use unchanged at ingest time.|
|MAX|Use unchanged at ingest time.|
|AVG|Use `SUM` and `COUNT` at ingest time. Switch to quotient of `SUM` at query time.|
|COUNT|Use unchanged at ingest time, but switch to `SUM` at query time.|
|COUNT(DISTINCT expr)|If approximate, use `APPROX_COUNT_DISTINCT` at ingest time.<br /><br />If exact, you cannot use an ingest-time aggregation. Instead, `expr` must be stored as-is. Add it to the `SELECT` and `GROUP BY` lists.|
|EARLIEST(expr)<br /><br />(numeric form)|Not supported.|
|EARLIEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|LATEST(expr)<br /><br />(numeric form)|Not supported.|
|LATEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_BUILTIN|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_HLL|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_THETA|Use unchanged at ingest time.|
|APPROX_QUANTILE|Not supported. Deprecated; we recommend using `APPROX_QUANTILE_DS` instead.|
|APPROX_QUANTILE_DS|Use `DS_QUANTILES_SKETCH` at ingest time. Continue using `APPROX_QUANTILE_DS` at query time.|
|APPROX_QUANTILE_FIXED_BUCKETS|Not supported.|

#### Multi-value dimensions

By default, multi-value dimensions are not ingested as expected when rollup is enabled, because the
GROUP BY operator unnests them instead of leaving them as arrays. This is [standard
behavior](https://druid.apache.org/docs/latest/querying/sql.html#multi-value-strings) for GROUP BY
in Druid queries, but is generally not desirable behavior for ingestion.

To address this:

- When using GROUP BY with data from EXTERN, wrap any `string`-typed fields from EXTERN that may be
  multi-valued in `MV_TO_ARRAY`.
- Set `groupByEnableMultiValueUnnesting: false` in your query context to ensure that all multi-value
  strings are properly converted to arrays using `MV_TO_ARRAY`. If any are encountered that are not
  wrapped in `MV_TO_ARRAY`, the query will report an error that includes the message "Encountered
  multi-value dimension x that cannot be processed with executingNestedQuery set to false." (Note:
  this message is incorrect; it should mention "groupByEnableMultiValueUnnesting", not
  "executingNestedQuery". This will be fixed in a future release.)

Check out the [INSERT with rollup example query](#example-insert-rollup) to see these features in
action.

## Performance

The main driver of performance is parallelism. The most relevant considerations are:

- The [`talariaNumTasks`](#context) query parameter determines the maximum number of
  worker tasks your query will use. Generally, queries will perform better with more workers.
- The EXTERN operator cannot split large files across different worker tasks. If you are reading a
  small number of large files, you can increase the parallelism of your worker tasks by splitting
  input files up beforehand.
- The `druid.worker.capacity`] server property on each Indexer or Middle Manager determines the
  maximum number of worker tasks that can run on that server at once.
- The `druid.processing.numThreads` server property on Indexers determines the maximum number of
  input files, partitions, or segments that can be processed at once on that server. This is unique
  to the Indexer. On Middle Managers, each worker task always runs in a single thread.

On Indexers, the processing thread pool is shared across all tasks running on the same server. This
has two important implications:

1. A single task, if it has enough input files, partitions, or segments, can access the full CPU
power of the server. For this reason, when using Indexers, adding more worker tasks on the same
server does not necessarily improve performance.
2. When multiple worker tasks for different queries run simultaneously on the same Indexer, each
worker task does not get guaranteed resources. For this reason, worker tasks for different queries
may need to wait for each other before gaining execution time on the shared processing thread pool.

Neither of these considerations apply to Middle Managers, since they do not use a shared processing
thread pool.

A secondary driver of performance is available memory. In two important cases — producer-side sort
as part of shuffle, and segment generation — more memory can reduce the number of passes required
through the data and therefore improve performance. For details, see the [Memory
usage](#memory-usage) section.

## Memory usage

Worker tasks launched by the multi-stage query engine use both JVM heap memory as well as off-heap
("direct") memory.

The bulk of the JVM heap (75%) is split up into equally-sized bundles. On Indexers, there is
one bundle for each processing thread (`druid.processing.numThreads`) and one bundle for each
worker that may run in the JVM (`druid.worker.capacity`). On Peons launched by MiddleManagers,
there are two bundles of equal size: one processor bundle and one worker bundle.

Processor memory bundles on the JVM heap are used for query processing and segment generation.
Each processor bundle also has space reserved for buffering frames from input channels: 1 MB
per worker from its input stages. Each processor is also allocated an off-heap processing
buffer of size `druid.processing.buffer.sizeBytes`.

Worker memory bundles on the JVM heap are used for sorting stage output data prior to shuffle.
Workers can sort more data than fits in memory; in this case, they will switch to using disk.

Increasing maximum heap size can speed up processing in two ways:

- Segment generation will become more efficient, as fewer spills to disk will be required.
- Sorting stage output data may become more efficient, since available memory affects the
  number of sorting passes that are required.

Worker tasks also use off-heap ("direct") memory. The amount of direct
memory available (`-XX:MaxDirectMemorySize`) should be set to at least
`(druid.processing.numThreads + 1) * druid.processing.buffer.sizeBytes`. Increasing the
amount of direct memory available beyond the minimum does not speed up processing.

It may be necessary to override one or more memory-related parameter if you run into one of the
current [known issues around memory usage](#known-issues-memory).

## Limits

Queries are subject to the following limits.

|Limit|Value|Error if exceeded|
|----|-----------|----|
| Size of an individual row written into a frame<br/><br/>Note: row size as written to a frame may differ from the original row size | 1 MB | RowTooLarge |
| Number of segment-granular time chunks encountered during ingestion | 5,000 | TooManyBuckets |
| Number of input files/segments per worker | 10,000| TooManyInputFiles |
| Number of output partitions for any one stage<br /> <br /> Number of segments generated during ingestion |25,000  |TooManyPartitions |
| Number of output columns for any one stage|  2,000| TooManyColumns|
| Number of workers for any one stage | 1,000 (hard limit)<br /><br />Memory-dependent (soft limit; may be lower) | TooManyWorkers |
| Maximum memory occupied by broadcasted tables | 30% of each [processor memory bundle](#memory-usage) |BroadcastTablesTooLarge |

## Security

> The security model for the multi-stage query engine is subject to change in future releases.
> Future releases may require different permissions than the current release.

In the current release, the security model for the multi-stage query engine is:

- All authenticated users are permitted to use the multi-stage query API if the [extension is
  loaded](#setup). However, without additional permissions, users are not able to issue queries that
  read or write Druid datasources or external data.
- Queries that SELECT from a Druid datasource require the READ DATASOURCE permission on that
  datasource.
- Queries that INSERT into a Druid datasource require the WRITE DATASOURCE permission on that
  datasource.
- Queries that use the EXTERN operator require the WRITE DATASOURCE permission on the
  `__you_have_been_visited_by_talaria` datasource. The purpose of this permission is to ensure that
  the user has write permission to _some_ datasource and is therefore permitted to use external
  input sources. No data will be inserted into this stub datasource.
- Multi-stage queries issued through the [Query API](#query-api) are associated with the user that
issued the query. Other users are not able to use the query API to retrieve status, results, or
details of queries they did not issue, and are not able to use the query API to cancel queries they
did not issue, even if those other users have permissions on the associated datasource. However:
multi-stage queries [run as indexing service tasks](#key-concepts), and the indexing service
security model _does_ permit other users to retrieve information about, or cancel, tasks that they
did not issue, as long as those other users have permissions on the associated datasource.

## Web console

There is a view added to the console specifically to support multi-stage query workflows. Please see
the annotated screenshot below.

![Annotated multi-stage query view](../assets/multi-stage-query/ui-annotated.png)

1. The multi-stage, tab-enabled, Query view is where all the new functionality is contained.
All other views are unchanged from their OSS versions. You can still access the original Query view by navigating to `#query` in the URL.
The tabs (3) serve as an indication that you are in the milti-stage query capable query view.
You can activate and deactivate this Query view by holding down `alt` (or `option`) and clicking the Druid logo in the top left corner of the console.

2. The resources view shows the available schemas, datasources, and columns just like in the original Query view.

3. Query tabs allow you to save, run, and manage several queries at once.
New tabs can be added via the `+` button.
Existing tabs can be manipulated via the `...` button on each tab.

4. The tab bar contains some helpful tools including a "Connect external data" button that creates an `EXTERN` helper query by sampling the data.

5. The tab bar wrench button contains miscellaneous tools that are yet to find a perfect home.
You can show/hide the Work history panel (6) and materialize and reabsorb the 
helper SQL queries (8).

6. The "Work history" panel lets you see previous queries executed by all users in the cluster.
It is equivalent to the task view in the Ingestion tab with the filter of `
type=’talaria0’`.
Work history panel can be shown/hidden from the toolbar wrench button (5).

7. You can click on each query entry to attach to that query to track its progress.
Additionally you can also show results, query stats, and the query that was issued.

8. The query helpers let you define notebook-style queries that can be reference from the main query as if they were defined as `WITH` clauses. You can refer to this extern 
by name anywhere in the SQL query where you could use a table.
They are essentially UI driven views that only exist within the given query tab.

9. Context comments can be inserted anywhere in the query to set context parameters via the query text.
This is not part of Druid’s API (although something similar might be 
one day). These comments are parsed out of the query text and added to the context object in the API payload.

10. The `Run` button’s more menu (`...`) lets you export the data as well as define the context for the query including the parallelism for the query.

11. The `Preview` button runs the query without the INSERT into clause and with an added LIMIT to the main query and to all helper queries.
The added LIMITs make the query run faster but could cause incomplete results.
The preview feature is only intended to show you the general shape of the data before inserting it.

12. The query timer indicates how long the query has been running for overall.

13. The `(cancel)` link cancels the currently running query.

14. The main progress bar shows the overall progress of the query.
The progress is computed from the various counters in the live reports (16).

15. The `Current stage` progress bar shows the progress for the currently running query stage.
If several stages are executing concurrently it conservatively shows the information for the earliest executing stage.

16. The live query reports show detailed information of all the stages (past, present, and future). The live reports are shown while the query is running (can be hidden). 
After the query finished they are available by clicking on the query time indicator or from the Work history panel (6)

17. Each stage of the live query reports can be expanded by clicking on the triangle to show per worker and per partition statistics.

## Example queries

### INSERT with no rollup <a href="#example-insert" name="example-insert">#</a>

```sql
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w000
SELECT
  TIME_PARSE("timestamp") AS __time,
  *
FROM TABLE(
    EXTERN(
      '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
      '{"type": "json"}',
      '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
    )
  )
PARTITIONED BY HOUR
CLUSTERED BY channel
```

### INSERT with rollup <a href="#example-insert-rollup" name="example-insert-rollup">#</a>

```sql
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO "kttm_rollup"

WITH kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  MV_TO_ARRAY("language") AS "language", -- Multi-value string dimension
  os,
  city,
  country,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_data
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
PARTITIONED BY HOUR
CLUSTERED BY browser, session
```

### INSERT for reindexing an existing datasource <a href="#example-reindexing" name="example-reindexing">#</a>

This query rolls up data from w000 and inserts the result into w002.

```sql
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w002
SELECT
  FLOOR(__time TO MINUTE) AS __time,
  channel,
  countryIsoCode,
  countryName,
  regionIsoCode,
  regionName,
  page,
  COUNT(*) AS cnt,
  SUM(added) AS sum_added,
  SUM(deleted) AS sum_deleted
FROM w000
GROUP BY 1, 2, 3, 4, 5, 6, 7
PARTITIONED BY HOUR
CLUSTERED BY page
```

### INSERT for reindexing an existing datasource into itself <a href="#example-self-reindexing" name="example-self-reindexing">#</a>

This query rolls up data for a specific day from w000, and does an insert-with-replacement into the
same table. Note that the `talariaReplaceTimeChunks` context parameter matches the WHERE filter.

```sql
--:context talariaReplaceTimeChunks: 2031-01-02/2031-01-03
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w000
SELECT
  FLOOR(__time TO MINUTE) AS __time,
  channel,
  countryIsoCode,
  countryName,
  regionIsoCode,
  regionName,
  page,
  COUNT(*) AS cnt,
  SUM(added) AS sum_added,
  SUM(deleted) AS sum_deleted
FROM w000
WHERE
  __time >= TIMESTAMP '2031-01-02 00:00:00'
  AND __time < TIMESTAMP '2031-01-03 00:00:00'
GROUP BY 1, 2, 3, 4, 5, 6, 7
PARTITIONED BY HOUR
CLUSTERED BY page
```

### INSERT with JOIN <a href="#example-insert-join" name="example-insert-join">#</a>

```sql
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w003
WITH wikidata AS (
  SELECT * FROM TABLE(
    EXTERN(
      '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
      '{"type": "json"}',
      '[{"name": "added", "type": "long"}, {"name": "channel", "type": "string"}, {"name": "cityName", "type": "string"}, {"name": "comment", "type": "string"}, {"name": "commentLength", "type": "long"}, {"name": "countryIsoCode", "type": "string"}, {"name": "countryName", "type": "string"}, {"name": "deleted", "type": "long"}, {"name": "delta", "type": "long"}, {"name": "deltaBucket", "type": "string"}, {"name": "diffUrl", "type": "string"}, {"name": "flags", "type": "string"}, {"name": "isAnonymous", "type": "string"}, {"name": "isMinor", "type": "string"}, {"name": "isNew", "type": "string"}, {"name": "isRobot", "type": "string"}, {"name": "isUnpatrolled", "type": "string"}, {"name": "metroCode", "type": "string"}, {"name": "namespace", "type": "string"}, {"name": "page", "type": "string"}, {"name": "regionIsoCode", "type": "string"}, {"name": "regionName", "type": "string"}, {"name": "timestamp", "type": "string"}, {"name": "user", "type": "string"}]'
    )
  )
),

countries AS (
  SELECT * FROM TABLE(
    EXTERN(
      '{"type": "http", "uris": ["https://static.imply.io/lookup/country.tsv"]}',
      '{"type": "tsv", "hasHeaderRow": true}',
      '[{ "name": "Country", "type": "string" },{ "name": "Capital", "type": "string" },{ "name": "ISO3", "type": "string" },{ "name": "ISO2", "type": "string" }]'
    )
  )
)

SELECT
  TIME_PARSE(wikidata."timestamp") AS __time,
  wikidata.*,
  countries.Capital AS countryCapital
FROM wikidata
LEFT JOIN countries ON wikidata.countryIsoCode = countries.ISO2
PARTITIONED BY HOUR
```

### SELECT with EXTERN and JOIN <a href="#example-select-extern-join" name="example-select-extern-join">#</a>

```sql
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

WITH flights AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/flights/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2005_11.csv.zip"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"depaturetime","type":"string"},{"name":"arrivalime","type":"string"},{"name":"Year","type":"long"},{"name":"Quarter","type":"long"},{"name":"Month","type":"long"},{"name":"DayofMonth","type":"long"},{"name":"DayOfWeek","type":"long"},{"name":"FlightDate","type":"string"},{"name":"Reporting_Airline","type":"string"},{"name":"DOT_ID_Reporting_Airline","type":"long"},{"name":"IATA_CODE_Reporting_Airline","type":"string"},{"name":"Tail_Number","type":"string"},{"name":"Flight_Number_Reporting_Airline","type":"long"},{"name":"OriginAirportID","type":"long"},{"name":"OriginAirportSeqID","type":"long"},{"name":"OriginCityMarketID","type":"long"},{"name":"Origin","type":"string"},{"name":"OriginCityName","type":"string"},{"name":"OriginState","type":"string"},{"name":"OriginStateFips","type":"long"},{"name":"OriginStateName","type":"string"},{"name":"OriginWac","type":"long"},{"name":"DestAirportID","type":"long"},{"name":"DestAirportSeqID","type":"long"},{"name":"DestCityMarketID","type":"long"},{"name":"Dest","type":"string"},{"name":"DestCityName","type":"string"},{"name":"DestState","type":"string"},{"name":"DestStateFips","type":"long"},{"name":"DestStateName","type":"string"},{"name":"DestWac","type":"long"},{"name":"CRSDepTime","type":"long"},{"name":"DepTime","type":"long"},{"name":"DepDelay","type":"long"},{"name":"DepDelayMinutes","type":"long"},{"name":"DepDel15","type":"long"},{"name":"DepartureDelayGroups","type":"long"},{"name":"DepTimeBlk","type":"string"},{"name":"TaxiOut","type":"long"},{"name":"WheelsOff","type":"long"},{"name":"WheelsOn","type":"long"},{"name":"TaxiIn","type":"long"},{"name":"CRSArrTime","type":"long"},{"name":"ArrTime","type":"long"},{"name":"ArrDelay","type":"long"},{"name":"ArrDelayMinutes","type":"long"},{"name":"ArrDel15","type":"long"},{"name":"ArrivalDelayGroups","type":"long"},{"name":"ArrTimeBlk","type":"string"},{"name":"Cancelled","type":"long"},{"name":"CancellationCode","type":"string"},{"name":"Diverted","type":"long"},{"name":"CRSElapsedTime","type":"long"},{"name":"ActualElapsedTime","type":"long"},{"name":"AirTime","type":"long"},{"name":"Flights","type":"long"},{"name":"Distance","type":"long"},{"name":"DistanceGroup","type":"long"},{"name":"CarrierDelay","type":"long"},{"name":"WeatherDelay","type":"long"},{"name":"NASDelay","type":"long"},{"name":"SecurityDelay","type":"long"},{"name":"LateAircraftDelay","type":"long"},{"name":"FirstDepTime","type":"string"},{"name":"TotalAddGTime","type":"string"},{"name":"LongestAddGTime","type":"string"},{"name":"DivAirportLandings","type":"string"},{"name":"DivReachedDest","type":"string"},{"name":"DivActualElapsedTime","type":"string"},{"name":"DivArrDelay","type":"string"},{"name":"DivDistance","type":"string"},{"name":"Div1Airport","type":"string"},{"name":"Div1AirportID","type":"string"},{"name":"Div1AirportSeqID","type":"string"},{"name":"Div1WheelsOn","type":"string"},{"name":"Div1TotalGTime","type":"string"},{"name":"Div1LongestGTime","type":"string"},{"name":"Div1WheelsOff","type":"string"},{"name":"Div1TailNum","type":"string"},{"name":"Div2Airport","type":"string"},{"name":"Div2AirportID","type":"string"},{"name":"Div2AirportSeqID","type":"string"},{"name":"Div2WheelsOn","type":"string"},{"name":"Div2TotalGTime","type":"string"},{"name":"Div2LongestGTime","type":"string"},{"name":"Div2WheelsOff","type":"string"},{"name":"Div2TailNum","type":"string"},{"name":"Div3Airport","type":"string"},{"name":"Div3AirportID","type":"string"},{"name":"Div3AirportSeqID","type":"string"},{"name":"Div3WheelsOn","type":"string"},{"name":"Div3TotalGTime","type":"string"},{"name":"Div3LongestGTime","type":"string"},{"name":"Div3WheelsOff","type":"string"},{"name":"Div3TailNum","type":"string"},{"name":"Div4Airport","type":"string"},{"name":"Div4AirportID","type":"string"},{"name":"Div4AirportSeqID","type":"string"},{"name":"Div4WheelsOn","type":"string"},{"name":"Div4TotalGTime","type":"string"},{"name":"Div4LongestGTime","type":"string"},{"name":"Div4WheelsOff","type":"string"},{"name":"Div4TailNum","type":"string"},{"name":"Div5Airport","type":"string"},{"name":"Div5AirportID","type":"string"},{"name":"Div5AirportSeqID","type":"string"},{"name":"Div5WheelsOn","type":"string"},{"name":"Div5TotalGTime","type":"string"},{"name":"Div5LongestGTime","type":"string"},{"name":"Div5WheelsOff","type":"string"},{"name":"Div5TailNum","type":"string"},{"name":"Unnamed: 109","type":"string"}]'
  )
)),
L_AIRPORT AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_AIRPORT.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"string"},{"name":"Description","type":"string"}]'
  )
)),
L_AIRPORT_ID AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_AIRPORT_ID.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"long"},{"name":"Description","type":"string"}]'
  )
)),
L_AIRLINE_ID AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_AIRLINE_ID.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"long"},{"name":"Description","type":"string"}]'
  )
)),
L_CITY_MARKET_ID AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_CITY_MARKET_ID.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"long"},{"name":"Description","type":"string"}]'
  )
)),
L_CANCELLATION AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_CANCELLATION.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"string"},{"name":"Description","type":"string"}]'
  )
)),
L_STATE_FIPS AS (
  SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/FlightCarrierOnTime/dimensions/L_STATE_FIPS.csv"]}',
    '{"type":"csv","findColumnsFromHeader":true}',
    '[{"name":"Code","type":"long"},{"name":"Description","type":"string"}]'
  )
))
SELECT
  depaturetime,
  arrivalime,
  -- "Year",
  -- Quarter,
  -- "Month",
  -- DayofMonth,
  -- DayOfWeek,
  -- FlightDate,
  Reporting_Airline,

  DOT_ID_Reporting_Airline,
  DOTAirlineLookup.Description AS DOT_Reporting_Airline,

  IATA_CODE_Reporting_Airline,
  Tail_Number,
  Flight_Number_Reporting_Airline,

  OriginAirportID,
  OriginAirportIDLookup.Description AS OriginAirport,

  OriginAirportSeqID,

  OriginCityMarketID,
  OriginCityMarketIDLookup.Description AS OriginCityMarket,

  Origin,
  OriginAirportLookup.Description AS OriginDescription,

  OriginCityName,
  OriginState,

  OriginStateFips,
  OriginStateFipsLookup.Description AS OriginStateFipsDescription,

  OriginStateName,
  OriginWac,

  DestAirportID,
  DestAirportIDLookup.Description AS DestAirport,

  DestAirportSeqID,

  DestCityMarketID,
  DestCityMarketIDLookup.Description AS DestCityMarket,

  Dest,
  DestAirportLookup.Description AS DestDescription,

  DestCityName,
  DestState,

  DestStateFips,
  DestStateFipsLookup.Description AS DestStateFipsDescription,

  DestStateName,
  DestWac,

  CRSDepTime,
  DepTime,
  DepDelay,
  DepDelayMinutes,
  DepDel15,
  DepartureDelayGroups,
  DepTimeBlk,
  TaxiOut,
  WheelsOff,
  WheelsOn,
  TaxiIn,
  CRSArrTime,
  ArrTime,
  ArrDelay,
  ArrDelayMinutes,
  ArrDel15,
  ArrivalDelayGroups,
  ArrTimeBlk,

  Cancelled,
  CancellationCode,
  CancellationCodeLookup.Description AS CancellationReason,

  Diverted,
  CRSElapsedTime,
  ActualElapsedTime,
  AirTime,
  Flights,
  Distance,
  DistanceGroup,
  CarrierDelay,
  WeatherDelay,
  NASDelay,
  SecurityDelay,
  LateAircraftDelay,
  FirstDepTime,
  TotalAddGTime,
  LongestAddGTime
FROM "flights"
LEFT JOIN L_AIRLINE_ID AS DOTAirlineLookup ON DOT_ID_Reporting_Airline = DOTAirlineLookup.Code
LEFT JOIN L_AIRPORT AS OriginAirportLookup ON Origin = OriginAirportLookup.Code
LEFT JOIN L_AIRPORT AS DestAirportLookup ON Dest = DestAirportLookup.Code
LEFT JOIN L_AIRPORT_ID AS OriginAirportIDLookup ON OriginAirportID = OriginAirportIDLookup.Code
LEFT JOIN L_AIRPORT_ID AS DestAirportIDLookup ON DestAirportID = DestAirportIDLookup.Code
LEFT JOIN L_CITY_MARKET_ID AS OriginCityMarketIDLookup ON OriginCityMarketID = OriginCityMarketIDLookup.Code
LEFT JOIN L_CITY_MARKET_ID AS DestCityMarketIDLookup ON DestCityMarketID = DestCityMarketIDLookup.Code
LEFT JOIN L_STATE_FIPS AS OriginStateFipsLookup ON OriginStateFips = OriginStateFipsLookup.Code
LEFT JOIN L_STATE_FIPS AS DestStateFipsLookup ON DestStateFips = DestStateFipsLookup.Code
LEFT JOIN L_CANCELLATION AS CancellationCodeLookup ON CancellationCode = CancellationCodeLookup.Code
LIMIT 1000
```

## Known issues

**Issues with general query execution.** <a href="#known-issues-general" name="known-issues-general">#</a>

- Fault tolerance is not yet implemented. If any task fails, the
  entire query fails. (14718)

- The controller task will stall if it cannot launch all worker tasks
  simultaneously. Additionally, two controller tasks can deadlock each
  other if the sum of their `talariaNumTasks` exceeds available task
  capacity. To avoid this, ensure that the sum of `talariaNumTasks`
  across all concurrently-running queries, plus the number of controller
  tasks (one per query), does not exceed available task capacity. (17183)

- On cancelation or failure due to exception, the controller shuts down its worker tasks as part of
  an orderly exit. However, worker tasks may outlive the controller in situations where the
  controller vanishes without a chance to run its shutdown routines. This can happen due to
  conditions like JVM crashes, OS crashes, or sudden hardware failure. (18052)

- Canceling the controller task sometimes leads to the error code
  "UnknownError" instead of "Canceled". (14722)

- The [Query details API](#get-query-details) may return
  `500 Server Error` or `404 Not Found` when the controller task is in
  process of starting up or shutting down. (15001)

- Only one local filesystem per server is used for stage output data during multi-stage query
  execution. If your servers have multiple local filesystems, this causes queries to exhaust
  available disk space earlier than expected. (16181)

- When [`groupByEnableMultiValueUnnesting: false`](#multi-value-dimensions) is set in the context of
  a query with GROUP BY, the generated error message is incorrect. It refers to its own parameter
  name as "executingNestedQuery" instead of "groupByEnableMultiValueUnnesting". (18156)

**Issues with memory usage.** <a href="#known-issues-memory" name="known-issues-memory">#</a>

- INSERT queries can consume excessive memory when using complex types due to inaccurate footprint
  estimation. This can appear as an OutOfMemoryError during the SegmentGenerator stage when using
  sketches. If you run into this issue, try manually lowering the value of the
  [`talariaRowsInMemory`](#context) parameter. (17946)

- INSERT queries can consume excessive memory on Indexers due to a too-high default value of
  `druid.processing.numThreads`. This can appear as an OutOfMemoryError during the SegmentGenerator
  stage. If you run into this issue, try manually setting this parameter to one less than the number
  of processors on the server. (18047)

- EXTERN loads an entire row group into memory at once when reading from Parquet files. Row groups
  can be up to 1 GB in size, which can lead to excessive heap usage when reading many files in
  parallel. This can appear as an OutOfMemoryError during stages that read Parquet input files. If
  you run into this issue, try using a smaller number of worker tasks or you can increase the heap
  size of your Indexers or of your Middle Manager-launched indexing tasks. (17932)

**Issues with SELECT queries.** <a href="#known-issues-select" name="known-issues-select">#</a>

- SELECT query results do not include realtime data until it has been published. (18092)

- SELECT query results are funneled through the controller task
  so they can be written to the [query report](#get-query-details).
  This is a bottleneck for queries with large resultsets. In the future,
  we will provide a mechanism for writing query results to multiple
  files in parallel. (14728)

- SELECT query results are materialized in memory on the Broker when
  using the [query results](#get-query-results) API. Large result sets
  can cause the Broker to run out of memory. (15963)

- TIMESTAMP types are formatted as numbers rather than ISO8601 timestamp
  strings, which differs from Druid's standard result format. (14995)

- BOOLEAN types are formatted as numbers like `1` and `0` rather
  than `true` or `false`, which differs from Druid's standard result
  format. (14995)

- TopN is not implemented. The context parameter
  `useApproximateTopN` is ignored and always treated as if it
  were `false`. Therefore, topN-shaped queries will
  always run using the groupBy engine. There is no loss of
  functionality, but there may be a performance impact, since
  these queries will run using an exact algorithm instead of an
  approximate one. (14998)

- GROUPING SETS is not implemented. Queries that use GROUPING SETS
  will fail. (14999)

- The numeric flavors of the EARLIEST and LATEST aggregators do not work
  properly. Attempting to use the numeric flavors of these aggregators will
  lead to an error like
  `java.lang.ClassCastException: class java.lang.Double cannot be cast to class org.apache.druid.collections.SerializablePair`.
  The string flavors, however, do work properly. (15040)

- When querying system tables in `INFORMATION_SCHEMA` or `sys`, the
  SQL API ignores the `talaria` parameter, and treats it as if it
  were false. These queries always run with the core Druid
  query engine. (15002)

**Issues with INSERT queries.** <a href="#known-issues-insert" name="known-issues-insert">#</a>

- The [schemaless dimensions](https://docs.imply.io/latest/druid/ingestion/ingestion-spec.html#inclusions-and-exclusions)
  feature is not available. All columns and their types must be
  specified explicitly. (15004)

- [Segment metadata queries](https://docs.imply.io/latest/druid/querying/segmentmetadataquery.html)
  on datasources ingested with the multi-stage query engine will return values for `rollup`,
  `queryGranularity`, `timestampSpec`, and `aggregators` that are not usable for introspection. In
  particular, Pivot will not be able to automatically create data cubes that properly reflect the
  rollup configurations of these datasources. Proper data cubes can still be created manually.
  (15006, 15007, 15008)

- When using INSERT with GROUP BY, the multi-stage engine generates segments that Druid's compaction
  functionality is not able to further roll up. This applies to autocompaction as well as manually
  issued `compact` tasks. Individual queries executed with the multi-stage engine always guarantee
  perfect rollup for their output, so this only matters if you are performing a sequence of INSERT
  queries that each append data to the same time chunk. If necessary, you can compact such data
  using another SQL query instead of a `compact` task. (17945)

- When using INSERT with GROUP BY, not all aggregation functions are implemented. See the
  [Rollup](#rollup) section for a list of aggregation functions that are currently available in
  conjuction with INSERT. Note that all aggregations are supported for SELECT queries. (15010)

- When using INSERT with GROUP BY, splitting of large partitions is not currently
  implemented. If a single partition key appears in a
  very large number of rows, an oversized segment will be created.
  You can mitigate this by adding additional columns to your
  partition key. Note that partition splitting _does_ work properly
  when performing INSERT without GROUP BY. (15015)

- INSERT with column lists, like
  `INSERT INTO tbl (a, b, c) SELECT ...`, is not implemented. (15009)

**Issues with EXTERN.** <a href="#known-issues-extern" name="known-issues-extern">#</a>

- EXTERN does not accept `druid` or `sql` input sources. (15016, 15018)

**Issues with missing guardrails.** <a href="#known-issues-guardrails" name="known-issues-guardrails">#</a>

- Maximum number of input files. No guardrail today means the controller can potentially run out of
  memory tracking them all. (15020)

- Maximum amount of local disk space to use for temporary data. No guardrail today means worker
  tasks may exhaust all available disk space. In this case, you will receive an
  [UnknownError](#errors) with a message including "No space left on device". (15022)

## Release notes

**2022.03** <a href="#2022.03" name="2022.03">#</a>

- The web console now includes counters for external input rows and files, Druid table input rows
  and segments, and sort progress. As part of this change, the query detail response format has
  changed. Clients performing programmatic access will need to be updated. (15048, 15208, 18070)
- Added the ability to avoid unnesting [multi-value string dimensions](#multi-value-dimensions)
  during GROUP BY. This is useful for performing ingestion with rollup. (15031, 16875, 16887)
- EXPLAIN PLAN FOR now works properly on INSERT queries. (17321)
- External input files are now read in parallel when running in Indexers. (17933)
- Improved accuracy of partition-determination. Segments generated by INSERT are now more regularly
  sized. (17867)
- [CannotParseExternalData](#errors) error reports now include input file path and line number
  information. (16016)
- There is now an upper limit on the number of workers, partially determined by available memory.
  Exceeding this limit leads to a [TooManyWorkers](#errors) error. (15021)
- There is now a guardrail on the maximum size of data involved in a broadcast join. Queries that
  exceed the limit will report a [BroadcastTablesTooLarge](#errors) error code. (15024)
- When a worker fails abruptly, the controller now reports a [WorkerTaskFailed](#errors) error
  code instead of UnknownError. (15024)
- Controllers will no longer give up on workers before the Overlord does. Previously, the controller
  would fail with the message "Connection refused" if workers took longer than 30 seconds to start
  up. (17602)
- Fixed an issue where INSERT queries that generate large numbers of time chunks may fail with a
  message containing "SketchesArgumentException: K must be >= 2 and <= 32768 and a power of 2". This
  happened when the number of generated time chunks was close to the [TooManyBuckets](#limits)
  limit. (14764)
- Fixed an issue where queries with certain input sources would report an error with the message
  "Too many workers" when there were more files than workers. (18022)
- Fixed an issue where SELECT queries with LIMIT would sometimes return more rows than intended.
  (17394)
- Fixed an issue where workers could intermittently fail with an UnknownError with the message "Invalid midstream marker". (17602)
- Fixed an issue where workers could run out of memory when connecting to large numbers of other
  workers. (16153)
- Fixed an issue where workers could run out of memory during GROUP BY of large external input
  files. (17781)
- Fixed an issue where workers could retry reading the same external input file repeatedly and never
  succeed. (17936, 18009)

**2022.02** <a href="#2022.02" name="2022.02">#</a>

- INSERT uses PARTITIONED BY and CLUSTERED BY instead of talariaSegmentGranularity and ORDER BY. (15045)
- INSERT validates the datasource name at planning time instead of execution time. (15038)
- SELECT queries support OFFSET. (15000)
- The "Connect external data" feature in the Query view of the web console correctly supports Parquet files.
  Previously, this feature would report a "ParseException: Incorrect Regex" error on Parquet files. (16197)
- The Query detail API includes startTime and durationMs for the whole query. (15046)

**2022.01** <a href="#2022.01" name="2022.01">#</a>

- Multi-stage queries can now be issued using the async query API provided by the
  [`imply-sql-async`](https://docs.imply.io/latest/druid/querying/sql-async-download-api/#submit-a-query)
  extension. (15014)
- New `talariaFinalizeAggregations` parameter may be set to false to cause queries to emit nonfinalized
  aggregation results. (15010)
- INSERT queries obtain minimally-sized locks rather than locking the entire target datasource. (15003)
- The Query view of the web console now has tabs and an engine selector that allows issuing multi-stage queries.
  The dedicated "Talaria" view has been removed.
- The web console includes an ingestion spec conversion tool. It performs a best-effort conversion of a native batch
  ingestion spec into a SQL query. It does not guarantee perfect fidelity, so we recommend that you
  review the generated SQL query before running it.
- INSERT queries with LIMIT and `talariaSegmentGranularity` set to "all" now execute properly and write a single
  segment to the target datasource. Previously, these queries would fail. (15051)
- INSERT queries using `talariaReplaceTimeChunks` now always produce "range" shard specs when appropriate. Previously,
  "numbered" shard specs would sometimes be produced instead of "range" shard specs as documented. (14768)

**2021.12** <a href="#2021.12" name="2021.12">#</a>

- Initial release.
