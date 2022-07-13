---
id: msqe-api
title: API
---

> The Multi-Stage Query Engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

> Earlier versions of the Multi-Stage Query Engine used the `/druid/v2/sql/async/` end point. The engine now uses different endpoints in version 2022.05 and later. Some actions use the `/druid/v2/sql/task` while others use the `/druid/indexer/v1/task/` endpoint . Additionally, you no longer need to set a context parameter for `talaria`. API calls to the `task` endpoint use the Multi-Stage Query Engine automatically.

During the preview phase, the enhanced Query view will provide the most stable experience. Use the UI if you do not need a programmatic interface.

Queries for the Multi-Stage Query Engine (MSQE) run as tasks. The action you want to take determines the endpoint you use:

- `/druid/v2/sql/task` endpoint: Submit a query for ingestion.
- `/druid/indexer/v1/task` endpoint: Interact with a query, including getting its status, getting its details, or canceling it.

## Submit a query

### Request

Submit queries using the `POST /druid/v2/sql/task/` API.

Currently, MSQE ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. SQL SELECT queries always behave if `resultFormat` is "array", `header` is
true, `typesHeader` is true, and `sqlTypesHeader` is true.

For queries like the examples in the [quickstart](./msqe-quickstart.md), you need to escape characters such as quotation marks (") if you use something like `curl`. If you use a method that can parse JSON seamlessly like Python, you don't need to. The following example does though.

The following example is the same query that you submit when you complete [Convert a JSON ingestion spec](./msqe-quickstart.md#convert-a-json-ingestion-spec) where you insert data into a table named `wikipedia`. Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/v2/sql/task
```

```json
{
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
      "msqMaxNumTasks": 3
  }
}
```

<!--curl-->

```bash
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "msqMaxNumTasks": 3
    }
```

<!--Python-->

```python
import json
import requests

url = "https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "msqMaxNumTasks": 3
  }
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

```

<!--END_DOCUSAURUS_CODE_TABS-->


### Response

```json
{
  "taskId": "query-f795a235-4dc7-4fef-abac-3ae3f9686b79",
  "state": "RUNNING",
}
```

**Response fields**

|Field|Description|
|-----|-----------|
|taskId|Controller task ID.<br /><br />Druid's standard [task APIs](../operations/api-reference.md#overlord) can be used to interact with this controller task.|
|state|Initial state for the query, which is "RUNNING".|

## Interact with a query

Because queries run as Overlord tasks, use the [task APIs](../operations/api-reference.md#overlord) to interact with a query.

When using MSQE, the endpoints you frequently use may include:

- `GET /druid/indexer/v1/task/{taskId}` to get the query payload
- `GET /druid/indexer/v1/task/{taskId}/status` to get the query status
- `GET /druid/indexer/v1/task/{taskId}/reports` to get the query report
- `POST /druid/indexer/v1/task/{taskId}/shutdown` to cancel a query

For information about the permissions required, see [Security](./msqe-security.md)

### MSQE reports

Keep the following in mind when using the task API to view reports:

- For SELECT queries, the results are included in the report. At this time, if you want to view results for SELECT queries, you need to retrieve them as a generic map from the report and extract the results.
- Query details are physically stored in the task report for controller tasks.
- If you encounter 500 Server Error or 404 Not Found errors, the task may be in the process of starting up or shutting down.



## Context variables

In addition to the Druid SQL [context parameters](../querying/sql-query-context.md), Multi-Stage Query Engine (MSQE) supports context variables that are specific to it.

You provide context variables alongside your queries to customize the behavior of the query. The way you provide the variables depends on how you submit your query:

- **Druid console**: Add your variables before your query like so:

   ```sql
   --:context <key>: <value> 
   --:context msqFinalizeAggregations: false
   INSERT INTO myTable
   ...
   ```

- **API**: Add them in a `context` section of your JSON object like so:

   ```sql
   {
    "query": "SELECT 1 + 1",
    "context": {
        "<key>": <value>,
        "msqMaxNumTasks": 3
    }
   }
   ```


|Parameter|Description|Default value|
|----|-----------|----|
| msqMaxNumTasks | (SELECT, INSERT, or REPLACE)<br /><br />The Multi-Stage Query Engine executes queries using the indexing service, that is using the Overlord + MiddleManager. This property specifies the maximum total number of tasks to launch, including the controller task.<br /><br />The lowest possible value for this setting is 2: one controller and one worker.<br /><br />All tasks must be able to launch simultaneously. If they cannot, the query will return a `TaskStartTimeout` error code after about 10 minutes.<br /><br />May also be provided as `msqNumTasks`. If both are present, `msqMaxNumTasks` takes priority.| 2 |
| msqTaskAssignment | (SELECT, INSERT, or REPLACE)<br /><br />Determines how the number of tasks is chosen.<br /><br />`max`: use as many tasks as possible, up to the maximum `msqMaxNumTasks`.<br /><br />`auto`: use as few tasks as possible without exceeding 10 Gib or 10,000 files per task. Please review the [limitations](./msqe-release.md#general-query-execution) of `auto` mode before using it.| `max` |
| msqFinalizeAggregations | (SELECT, INSERT, or REPLACE)<br /><br />Whether Druid will finalize the results of complex aggregations that directly appear in query results.<br /><br />If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | true |
| msqRowsInMemory | (INSERT or REPLACE)<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries.<br /><br />In most cases, you should stick to the default. It may be necessary to override this if you run into one of the current [known issues around memory usage](./msqe-release.md#memory-usage)</a>. | 100,000 |
| msqSegmentSortOrder | (INSERT or REPLACE)<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, then the [CLUSTERED BY](./msqe-sql-syntax.md#clustered-by) clause. When `msqSegmentSortOrder` is set, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />The column list can be provided as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`.<br /><br />For example: consider an INSERT query that uses `CLUSTERED BY country` and has `msqSegmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| maxParseExceptions| (SELECT, INSERT, or REPLACE)<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.<br /><br />| 0 |
| msqMode| (SELECT, INSERT, or REPLACE)<br /><br />Execute a query with a predefined set of parameters which are pretuned. It can be set to `strict` or `nonStrict`. Setting the mode to `strict` is equivalent to setting `maxParseExceptions: 0`: the query fails if there is a malformed record. Setting the mode to `nonStrict` is equivalent to setting `maxParseExceptions: -1`: the query won't fail regardless of the number of malformed records.  .<br /><br />| no default value |
| msqRowsPerSegment        | (INSERT or REPLACE)<br /><br />Number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, you should stick to the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| msqDurableShuffleStorage | (SELECT, INSERT, or REPLACE) <br /><br />Whether to use durable storage for shuffle mesh. To use this feature, durable storage must be configured at the server level using `druid.msq.intermediate.storage.enable=true` and its [associated properties](./msqe-advanced-configs.md#durable-storage-for-mesh-shuffle). If these properties are not configured, any query with the context variable `msqDurableShuffleStorage=true` fails with a configuration error. <br /><br /> | false |

## Report response fields

The following table describes the response fields when you retrieve a report for a MSQE task using the `/druid/indexer/v1/task` endpoint:

|Field|Description|
|-----|-----------|
|multiStageQuery.taskId|Controller task ID.|
|multiStageQuery.payload.status|Query status container.|
|multiStageQuery.payload.status.status|RUNNING, SUCCESS, or FAILED.|
|multiStageQuery.payload.status.startTime|Start time of the query in ISO format. Only present if the query has started running.|
|multiStageQuery.payload.status.durationMs|Milliseconds elapsed after the query has started running. -1 denotes that the query hasn't started running yet.|
|multiStageQuery.payload.status.errorReport|Error object. Only present if there was an error.|
|multiStageQuery.payload.status.errorReport.taskId|The task that reported the error, if known. May be a controller task or a worker task.|
|multiStageQuery.payload.status.errorReport.host|The hostname and port of the task that reported the error, if known.|
|multiStageQuery.payload.status.errorReport.stageNumber|The stage number that reported the error, if it happened during execution of a specific stage.|
|multiStageQuery.payload.status.errorReport.error|Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorCode|One of the error codes from the [error code table](#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorMessage|User-friendly error message. Not always present, even if there is an error.|
|multiStageQuery.payload.status.errorReport.exceptionStackTrace|Java stack trace in string form, if the error was due to a server-side exception.|
|multiStageQuery.payload.stages|Array of query stages.|
|multiStageQuery.payload.stages[].stageNumber|Each stage has a number that differentiates it from other stages.|
|multiStageQuery.payload.stages[].inputStages|Array of input stage numbers.|
|multiStageQuery.payload.stages[].stageType|String that describes the logic of this stage. This is not necessarily unique across stages.|
|multiStageQuery.payload.stages[].phase|Either NEW, READING_INPUT, POST_READING, RESULTS_COMPLETE, or FAILED. Only present if the stage has started.|
|multiStageQuery.payload.stages[].workerCount|Number of parallel tasks that this stage is running on. Only present if the stage has started.|
|multiStageQuery.payload.stages[].partitionCount|Number of output partitions generated by this stage. Only present if the stage has started and has computed its number of output partitions.|
|multiStageQuery.payload.stages[].inputFileCount|Number of external input files or Druid segments read by this stage. Does not include inputs from other stages in the same query.|
|multiStageQuery.payload.stages[].startTime|Start time of this stage. Only present if the stage has started.|
|multiStageQuery.payload.stages[].query|Native Druid query for this stage. Only present for the first stage that corresponds to a particular native Druid query.|

## Error codes

The following table describes error codes you may encounter in the `multiStageQuery.payload.status.errorReport.error.errorCode` field when using MSQE:

|Code|Meaning|Additional fields|
|----|-----------|----|
|  BroadcastTablesTooLarge  | Size of the broadcast tables used in right hand side of the joins exceeded the memory reserved for them in a worker task  | &bull;&nbsp;maxBroadcastTablesSize: Memory reserved for the broadcast tables, measured in bytes |
|  Canceled  |  The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>|    |
|  CannotParseExternalData  |  A worker task could not parse data from an external datasource.  |    |
|  DurableStorageConfiguration  | Durable storage mode could not be activated due to a misconfiguration.<br /><br /> Check [durable storage for shuffle mesh](./msqe-advanced-configs.md#durable-storage-for-mesh-shuffle) for instructions on configuration.  |  |
|  ColumnTypeNotSupported  |  The query tried to use a column type that is not supported by the frame format.<br /><br />This currently occurs with ARRAY types, which are not yet implemented for frames.  | &bull;&nbsp;columnName<br /> <br />&bull;&nbsp;columnType   |
|  InsertCannotAllocateSegment  |  The controller task could not allocate a new segment ID due to conflict with pre-existing segments or pending segments. Two common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>|   &bull;&nbsp;dataSource<br /> <br />&bull;&nbsp;interval: interval for the attempted new segment allocation  |
|  InsertCannotBeEmpty  |  An INSERT or REPLACE query did not generate any output rows, in a situation where output rows are required for success.<br /> <br />Can happen for INSERT or REPLACE queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`.  |  &bull;&nbsp;dataSource  |
|  InsertCannotOrderByDescending  |  An INSERT query contained an `CLUSTERED BY` expression with descending order.<br /> <br />Currently, Druid's segment generation code only supports ascending order.  |   &bull;&nbsp;columnName |
|  InsertCannotReplaceExistingSegment  |  A REPLACE query cannot proceed because an existing segment partially overlaps those bounds and the portion within those bounds is not fully overshadowed by query results. <br /> <br />There are two ways to address this without modifying your query:<ul><li>Shrink the OVERLAP filter to match the query results.</li><li>Expand the OVERLAP filter to fully contain the existing segment.</li></ul>| &bull;&nbsp;segmentId: the existing segment <br /> 
|  InsertLockPreempted  | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a realtime ingestion task.  | |
|  InsertTimeNull  | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with an unparseable timestamp. (TIME_PARSE returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern.<br /><br />If your timestamps may genuinely be null, consider using COALESCE to provide a default value. One option is CURRENT_TIMESTAMP, which represents the start time of the job. |
|  InsertTimeOutOfBounds  |  A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWHERE WHERE clause.<br /> <br />To avoid this error, verify that the timeframe you specified is valid.  |  &bull;&nbsp;interval: time chunk interval corresponding to the out-of-bounds timestamp  |
| QueryNotSupported   |   QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that the multi-stage engine does not yet support, like GROUPING SETS. |    |
|  RowTooLarge  |  The query tried to process a row that was too large to write to a single frame.<br /> <br />See the Limits table for the specific limit on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing.  |   &bull;&nbsp;maxFrameSize: the limit on frame size which was exceeded |
|  TaskStartTimeout  | Unable to launch all the worker tasks in time. <br /> <br />There might be insufficient available slots to start all the worker tasks simultaneously.<br /> <br /> Try splitting up the query into smaller chunks with lesser `msqMaxNumTasks` number. Another option is to increase capacity.  | |
|  TooManyBuckets  |  Too many partition buckets for a stage.<br /> <br />Currently, partition buckets are only used for segmentGranularity during INSERT queries. The most common reason for this error is that your segmentGranularity is too narrow relative to the data. See the [Limits](./msqe-advanced-configs.md#limits) table for the specific limit.  |  &bull;&nbsp;maxBuckets: the limit on buckets which was exceeded  |
| TooManyInputFiles | Too many input files/segments per worker.<br /> <br />See the [Limits](./msqe-advanced-configs.md#limits) table for the specific limit. |&bull;&nbsp;numInputFiles: the total number of input files/segments for the stage<br /><br />&bull;&nbsp;maxInputFiles: the maximum number of input files/segments per worker per stage<br /><br />&bull;&nbsp;minNumWorker: the minimum number of workers required for a sucessfull run |
|  TooManyPartitions   |  Too many partitions for a stage.<br /> <br />The most common reason for this is that the final stage of an INSERT or REPLACE query generated too many segments. See the [Limits](./msqe-advanced-configs.md#limits) table for the specific limit.  | &bull;&nbsp;maxPartitions: the limit on partitions which was exceeded    |
|  TooManyColumns |  Too many output columns for a stage.<br /> <br />See the [Limits](./msqe-advanced-configs.md#limits) table for the specific limit.  | &bull;&nbsp;maxColumns: the limit on columns which was exceeded   |
|  TooManyWarnings |  Too many warnings of a particular type generated. | &bull;&nbsp;errorCode: The errorCode corresponding to the exception that exceeded the required limit. <br /><br />&bull;&nbsp;maxWarnings: Maximum number of warnings that are allowed for the corresponding errorCode.   |
|  TooManyWorkers |  Too many workers running simultaneously.<br /> <br />See the [Limits](./msqe-advanced-configs.md#limits) table for the specific limit.  | &bull;&nbsp;workers: a number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage, if multiple stages are running simultaneously. <br /><br />&bull;&nbsp;maxWorkers: the hard or soft limit on workers which was exceeded   |
|  NotEnoughMemory  |  Not enough memory to launch a stage.  |  &bull;&nbsp;serverMemory: the amount of memory available to a single process<br /><br />&bull;&nbsp;serverWorkers: the number of workers running in a single process<br /><br />&bull;&nbsp;serverThreads: the number of threads in a single process  |
|  WorkerFailed  |  A worker task failed unexpectedly.  |  &bull;&nbsp;workerTaskId: the id of the worker task  |
|  WorkerRpcFailed  |  A remote procedure call to a worker task failed unrecoverably.  |  &bull;&nbsp;workerTaskId: the id of the worker task  |
|  UnknownError   |  All other errors.  |    |