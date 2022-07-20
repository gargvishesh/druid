---
id: concepts
title: Concepts
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

This topic covers the main concepts and terminology of the Multi-Stage Query (MSQ) Framework.

## Vocabulary

You might see the following terms in the documentation or while you're using MSQ, such as when you view a report about a query:

- **Controller**: An indexing service task of type `query_controller` that manages
  the execution of a query. There is one controller task per query.

- **Worker**: Indexing service tasks of type `query_worker` that execute a
  query. There can be multiple worker tasks per query. Internally,
  the tasks process items in parallel using their processing pools.
  (i.e., up to `druid.processing.numThreads` of execution parallelism
  within a worker task).

- **Stage**: A stage of query execution that is parallelized across
  worker tasks. Workers exchange data with each other between stages.

- **Partition**: A slice of data output by worker tasks. In INSERT or REPLACE
  queries, the partitions of the final stage become Druid segments.

- **Shuffle**: Workers exchange data between themselves on a per-partition basis in a process called
  shuffling. During a shuffle, each output partition is sorted by a clustering key.

## How MSQ works

Queries execute using indexing service tasks, specifically INSERT, REPLACE, and SELECT queries. Every query occupies at least two task slots while running. 

When you submit a task query to MSQ, the following happens:

1.  The Broker plans your SQL query into a native query, as usual.

2.  The Broker wraps the native query into a task of type `query_controller`
    and submits it to the indexing service.

3. The Broker returns the task ID to you and exits.

4.  The controller task launches some number of worker tasks determined by
    the `msqMaxNumTasks` and `msqTaskAssignment` [context parameters](./msq-reference.md#context-parameters). You can set these settings individually for each query.

5.  The worker tasks execute the query.

6.  If the query is a SELECT query, the worker tasks send the results
    back to the controller task, which writes them into its task report.
    If the query is an INSERT or REPLACE query, the worker tasks generate and
    publish new Druid segments to the provided datasource.


## Parallelism

Paralleism affects how MSQ performs.

The [`msqMaxNumTasks`](./msq-reference.md#context-parameters) query parameter determines the maximum number of tasks (workers and one controller) your query will use. Generally, queries perform better with more workers. The lowest possible value of `msqMaxNumTasks` is two (one worker and one controller), and the highest possible value is equal to the number of free task slots in your cluster.

The `druid.worker.capacity` server property on each Middle Manager determines the maximum number
of worker tasks that can run on each server at once. Worker tasks run single-threaded, which
also determines the maximum number of processors on the server that can contribute towards
multi-stage queries. In Imply Enterprise, where data servers are shared between Historicals and
Middle Managers, the default setting for `druid.worker.capacity` is lower than the number of
processors on the server. Advanced users may consider enhancing parallelism by increasing this
value to one less than the number of processors on the server. In most cases, this increase must
be accompanied by an adjustment of the memory allotment of the Historical process,
Middle-Manager-launched tasks, or both, to avoid memory overcommitment and server instability. If
you are not comfortable tuning these memory usage parameters to avoid overcommitment, it is best
to stick with the default `druid.worker.capacity`.

## Memory usage

In two important cases, producer-side sort as part of shuffle and segment generation, more memory can reduce the number of passes required through the data and therefore improve performance.

Worker tasks launched by MSQ use both JVM heap memory and off-heap ("direct") memory.

On Peons launched by Middle Managers, the bulk of the JVM heap (75%) is split up into two
equally-sized bundles: one processor bundle and one worker bundle. Each one comprises 37.5% of the
available JVM heap.

The processor memory bundle is used for query processing and segment generation. Each processor bundle must
also provides space to buffer I/O between stages: each downstream stage requires 1MB of buffer space for
each upstream worker. For example, if you have 100 workers running in stage 0, and stage 1 reads from stage 0,
then each worker in stage 1 requires 1M * 100 = 100MB of memory for frame buffers.

The worker memory bundle is used for sorting stage output data prior to shuffle. Workers can sort
more data than fits in memory; in this case, they will switch to using disk.

Increasing maximum heap size can speed up processing in two ways:

- Segment generation will become more efficient, as fewer spills to disk will be required.
- Sorting stage output data may become more efficient, since available memory affects the
  number of sorting passes that are required.

Worker tasks also use off-heap ("direct") memory. The amount of direct
memory available (`-XX:MaxDirectMemorySize`) should be set to at least
`(druid.processing.numThreads + 1) * druid.processing.buffer.sizeBytes`. Increasing the
amount of direct memory available beyond the minimum does not speed up processing.

It may be necessary to override one or more memory-related parameter if you run into one of the
current [known issues around memory usage](./msq-release.md#memory-usage).


## Limits

Understanding the limits of MSQ can help you troubleshoot any [errors](#error-codes) that you encounter since many of the errors occur because an MSQ limit is reached. Queries are subject to the following limits:

|Limit|Value|Error if exceeded|
|----|-----------|----|
| Size of an individual row written into a frame<br/><br/>Note: row size as written to a frame may differ from the original row size | 1 MB | RowTooLarge |
| Number of segment-granular time chunks encountered during ingestion | 5,000 | TooManyBuckets |
| Number of input files/segments per worker | 10,000| TooManyInputFiles |
| Number of output partitions for any one stage<br /> <br /> Number of segments generated during ingestion |25,000  |TooManyPartitions |
| Number of output columns for any one stage|  2,000| TooManyColumns|
| Number of workers for any one stage | 1,000 (hard limit)<br /><br />Memory-dependent (soft limit; may be lower) | TooManyWorkers |
| Maximum memory occupied by broadcasted tables | 30% of each [processor memory bundle](#memory-usage) |BroadcastTablesTooLarge |

## Error codes

The following table describes error codes you may encounter in the `multiStageQuery.payload.status.errorReport.error.errorCode` field when using MSQ:

|Code|Meaning|Additional fields|
|----|-----------|----|
|  BroadcastTablesTooLarge  | Size of the broadcast tables used in right hand side of the joins exceeded the memory reserved for them in a worker task  | &bull;&nbsp;maxBroadcastTablesSize: Memory reserved for the broadcast tables, measured in bytes |
|  Canceled  |  The query was canceled. Common reasons for cancellation:<br /><br /><ul><li>User-initiated shutdown of the controller task via the `/druid/indexer/v1/task/{taskId}/shutdown` API.</li><li>Restart or failure of the server process that was running the controller task.</li></ul>|    |
|  CannotParseExternalData  |  A worker task could not parse data from an external datasource.  |    |
|  DurableStorageConfiguration  | Durable storage mode could not be activated due to a misconfiguration.<br /><br /> Check [durable storage for shuffle mesh](./msq-advanced-configs.md#durable-storage-for-mesh-shuffle) for instructions on configuration.  |  |
|  ColumnTypeNotSupported  |  The query tried to use a column type that is not supported by the frame format.<br /><br />This currently occurs with ARRAY types, which are not yet implemented for frames.  | &bull;&nbsp;columnName<br /> <br />&bull;&nbsp;columnType   |
|  InsertCannotAllocateSegment  |  The controller task could not allocate a new segment ID due to conflict with pre-existing segments or pending segments. Two common reasons for such conflicts:<br /> <br /><ul><li>Attempting to mix different granularities in the same intervals of the same datasource.</li><li>Prior ingestions that used non-extendable shard specs.</li></ul>|   &bull;&nbsp;dataSource<br /> <br />&bull;&nbsp;interval: interval for the attempted new segment allocation  |
|  InsertCannotBeEmpty  |  An INSERT or REPLACE query did not generate any output rows, in a situation where output rows are required for success.<br /> <br />Can happen for INSERT or REPLACE queries with `PARTITIONED BY` set to something other than `ALL` or `ALL TIME`.  |  &bull;&nbsp;dataSource  |
|  InsertCannotOrderByDescending  |  An INSERT query contained an `CLUSTERED BY` expression with descending order.<br /> <br />Currently, Druid's segment generation code only supports ascending order.  |   &bull;&nbsp;columnName |
|  InsertCannotReplaceExistingSegment  |  A REPLACE query cannot proceed because an existing segment partially overlaps those bounds and the portion within those bounds is not fully overshadowed by query results. <br /> <br />There are two ways to address this without modifying your query:<ul><li>Shrink the OVERLAP filter to match the query results.</li><li>Expand the OVERLAP filter to fully contain the existing segment.</li></ul>| &bull;&nbsp;segmentId: the existing segment <br /> 
|  InsertLockPreempted  | An INSERT or REPLACE query was canceled by a higher-priority ingestion job, such as a realtime ingestion task.  | |
|  InsertTimeNull  | An INSERT or REPLACE query encountered a null timestamp in the `__time` field.<br /><br />This can happen due to using an expression like `TIME_PARSE(timestamp) AS __time` with an unparseable timestamp. (TIME_PARSE returns null when it cannot parse a timestamp.) In this case, try parsing your timestamps using a different function or pattern.<br /><br />If your timestamps may genuinely be null, consider using COALESCE to provide a default value. One option is CURRENT_TIMESTAMP, which represents the start time of the job. |
|  InsertTimeOutOfBounds  |  A REPLACE query generated a timestamp outside the bounds of the TIMESTAMP parameter for your OVERWHERE WHERE clause.<br /> <br />To avoid this error, verify that the timeframe you specified is valid.  |  &bull;&nbsp;interval: time chunk interval corresponding to the out-of-bounds timestamp  |
+|  InvalidNullByte  | A string column included a null byte. Null bytes in strings are not permitted. |  &bull;&nbsp;column: the column that included the null byte |
| QueryNotSupported   |   QueryKit could not translate the provided native query to a multi-stage query.<br /> <br />This can happen if the query uses features that the multi-stage engine does not yet support, like GROUPING SETS. |    |
|  RowTooLarge  |  The query tried to process a row that was too large to write to a single frame.<br /> <br />See the Limits table for the specific limit on frame size. Note that the effective maximum row size is smaller than the maximum frame size due to alignment considerations during frame writing.  |   &bull;&nbsp;maxFrameSize: the limit on frame size which was exceeded |
|  TaskStartTimeout  | Unable to launch all the worker tasks in time. <br /> <br />There might be insufficient available slots to start all the worker tasks simultaneously.<br /> <br /> Try splitting up the query into smaller chunks with lesser `msqMaxNumTasks` number. Another option is to increase capacity.  | |
|  TooManyBuckets  |  Too many partition buckets for a stage.<br /> <br />Currently, partition buckets are only used for segmentGranularity during INSERT queries. The most common reason for this error is that your segmentGranularity is too narrow relative to the data. See the [Limits](./msq-concepts.md#limits) table for the specific limit.  |  &bull;&nbsp;maxBuckets: the limit on buckets which was exceeded  |
| TooManyInputFiles | Too many input files/segments per worker.<br /> <br />See the [Limits](./msq-concepts.md#limits) table for the specific limit. |&bull;&nbsp;numInputFiles: the total number of input files/segments for the stage<br /><br />&bull;&nbsp;maxInputFiles: the maximum number of input files/segments per worker per stage<br /><br />&bull;&nbsp;minNumWorker: the minimum number of workers required for a sucessfull run |
|  TooManyPartitions   |  Too many partitions for a stage.<br /> <br />The most common reason for this is that the final stage of an INSERT or REPLACE query generated too many segments. See the [Limits](./msq-concepts.md#limits) table for the specific limit.  | &bull;&nbsp;maxPartitions: the limit on partitions which was exceeded    |
|  TooManyColumns |  Too many output columns for a stage.<br /> <br />See the [Limits](./msq-concepts.md#limits) table for the specific limit.  | &bull;&nbsp;maxColumns: the limit on columns which was exceeded   |
|  TooManyWarnings |  Too many warnings of a particular type generated. | &bull;&nbsp;errorCode: The errorCode corresponding to the exception that exceeded the required limit. <br /><br />&bull;&nbsp;maxWarnings: Maximum number of warnings that are allowed for the corresponding errorCode.   |
|  TooManyWorkers |  Too many workers running simultaneously.<br /> <br />See the [Limits](./msq-concepts.md#limits) table for the specific limit.  | &bull;&nbsp;workers: a number of simultaneously running workers that exceeded a hard or soft limit. This may be larger than the number of workers in any one stage, if multiple stages are running simultaneously. <br /><br />&bull;&nbsp;maxWorkers: the hard or soft limit on workers which was exceeded   |
|  NotEnoughMemory  |  Not enough memory to launch a stage.  |  &bull;&nbsp;serverMemory: the amount of memory available to a single process<br /><br />&bull;&nbsp;serverWorkers: the number of workers running in a single process<br /><br />&bull;&nbsp;serverThreads: the number of threads in a single process  |
|  WorkerFailed  |  A worker task failed unexpectedly.  |  &bull;&nbsp;workerTaskId: the id of the worker task  |
|  WorkerRpcFailed  |  A remote procedure call to a worker task failed unrecoverably.  |  &bull;&nbsp;workerTaskId: the id of the worker task  |
|  UnknownError   |  All other errors.  |    |
