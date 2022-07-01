---
id: msqe-advanced
title: Advanced configs
---

> The Multi-Stage Query Engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.


## Durable storage for mesh shuffle

To use durable storage for mesh shuffles, 

- [Enable durable storage for mesh shuffle](./msqe-setup.md)
- Include the following context variable when you submit a query:

**UI**

   ```sql
   --:context msqDurableShuffleStorage: true
   ```

**API**

   ```json
   "context": {
       "msqDurableShuffleStorage": true
   }
   ```

The following table describes the properties used to configure durable storage for MSQE:

| Config  | Description  | Required | Default |
|---------|--------------------------|----------|---------|
| `druid.msq.intermediate.storage.enable`                      | Set to `true` to enable this feature.  | Yes      | None    |
| `druid.msq.intermediate.storage.type`                      | Must be set to `s3`.  | Yes      | None    |
| `druid.msq.intermediate.storage.bucket`                    | S3 bucket to store intermediate stage. results.  | Yes      | None  |
| `druid.msq.intermediate.storage.prefix`                    | S3 prefix to store intermediate stage results. Provide a unique value for the prefix. Clusters should not share the same prefix.  | Yes      | None |
| `druid.msq.intermediate.storage.tempDir`                   | Directory path on the local disk to store intermediate stage results. temporarily.  | Yes      | None |
| `druid.msq.intermediate.storage.maxResultsSize`            | Max size of each partition file per stage. It should be between 5MiB and 5TiB. Supports a human-readable format.  For eg if a stage has 50 partitions we can effectively use s3 up to 250TIB of stage output assuming each partition file <=5TiB. | No       | 100MiB  |
| `druid.msq.intermediate.storage.chunkSize`                 | Imply recommends using the default value for most cases. This property defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. Druid computes the chunk size automatically if this property is not set. The chunk size must be between 5MiB and 5GiB. | No       | None    |
| `druid.msq.intermediate.storage.maxTriesOnTransientErrors` | Imply recommends using the default value for most cases. This property defines the max number times to attempt S3 API calls to avoid failures due to transient errors.  | no       | 10      |

## Performance

The main driver of performance is parallelism. A secondary driver of performance is available memory.

### Parallelism

The most relevant considerations are:

- The [`msqNumTasks`](./msqe-api.md#context-variables) query parameter determines the maximum number of tasks (workers and one controller) your query will use. Generally, queries perform better with more workers. The lowest possible value of `msqNumTasks` is two (one worker and one controller), and the highest possible value is equal to the number of free task slots in your cluster.
- The EXTERN operator cannot split large files across different worker tasks. If you have fewer
  input files than worker tasks, you can increase query parallelism by splitting up your input
  files such that you have at least one input file per worker task.
- The `druid.worker.capacity` server property on each Middle Manager determines the maximum number
  of worker tasks that can run on each server at once. Worker tasks run single-threaded, so this
  also determines the maximum number of processors on the server that can contribute towards
  multi-stage queries. In Imply Enterprise, where data servers are shared between Historicals and
  Middle Managers, the default setting for `druid.worker.capacity` is lower than the number of
  processors on the server. Advanced users may consider enhancing parallelism by increasing this
  value to one less than the number of processors on the server. In most cases, this increase must
  be accompanied by an adjustment of the memory allotment of the Historical process,
  Middle-Manager-launched tasks, or both, to avoid memory overcommitment and server instability. If
  you are not comfortable tuning these memory usage parameters to avoid overcommitment, it is best
  to stick with the default `druid.worker.capacity`.

### Memory usage

In two important cases, producer-side sort as part of shuffle and segment generation, more memory can reduce the number of passes required through the data and therefore improve performance.

Worker tasks launched by MSQE use both JVM heap memory and off-heap ("direct") memory.

On Peons launched by Middle Managers, the bulk of the JVM heap (75%) is split up into two
equally-sized bundles: one processor bundle and one worker bundle. Each one comprises 37.5% of the
available JVM heap.

The processor memory bundle is used for query processing and segment generation. Each processor bundle must also provides space to buffer I/O between stages: each "downstream" stage requires 1MB of buffer space for each "upstream" stage. For example, if you have 100 input stages (msqNumTasks = 100, and you have at least 100 input files), then each second-stage worker will require 1M * 100 = 100MB of memory for frame buggers.

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
current [known issues around memory usage](./msqe-release.md#memory-usage).

## Limits

Queries are subject to the following limits:

|Limit|Value|Error if exceeded|
|----|-----------|----|
| Size of an individual row written into a frame<br/><br/>Note: row size as written to a frame may differ from the original row size | 1 MB | RowTooLarge |
| Number of segment-granular time chunks encountered during ingestion | 5,000 | TooManyBuckets |
| Number of input files/segments per worker | 10,000| TooManyInputFiles |
| Number of output partitions for any one stage<br /> <br /> Number of segments generated during ingestion |25,000  |TooManyPartitions |
| Number of output columns for any one stage|  2,000| TooManyColumns|
| Number of workers for any one stage | 1,000 (hard limit)<br /><br />Memory-dependent (soft limit; may be lower) | TooManyWorkers |
| Maximum memory occupied by broadcasted tables | 30% of each [processor memory bundle](#memory-usage) |BroadcastTablesTooLarge |



## How MSQE works

This section describes what happens when you submit a query to MSQE. 

The Multi-Stage Query Engine extends Druid's query stack to handle asynchronous queries that can exchange data between stages.

Queries execute using indexing service tasks, specifically INSERT, REPLACE, and SELECT queries. Every query occupies at least two task slots while running. 

Key concepts for multi-stage query execution:

- **Controller**: an indexing service task of type `query_controller` that manages
  the execution of a query. There is one controller task per query.

- **Worker**: indexing service tasks of type `query_worker` that execute a
  query. There may be more than one worker task per query. Internally,
  the tasks process items in parallel using their processing pools.
  (i.e., up to `druid.processing.numThreads` of execution parallelism
  within a worker task).

- **Stage**: a stage of query execution that is parallelized across
  worker tasks. Workers exchange data with each other between stages.

- **Partition**: a slice of data output by worker tasks. In INSERT or REPLACE
  queries, the partitions of the final stage become Druid segments.

- **Shuffle**: workers exchange data between themselves on a per-partition basis in a process called
  "shuffling". During a shuffle, each output partition is sorted by a clustering key.

When you use the Multi-Stage Query Engine, the following happens:

1.  The **Broker** plans your SQL query into a native query, as usual.

2.  The Broker wraps the native query into a task of type ``query_controller`
    and submits it to the indexing service. The Broker returns the task
    ID to you and exits.

3.  The **controller task** launches `msqNumTasks -1` **worker tasks**. 

4.  The worker tasks execute the query.

5.  If the query is a SELECT query, the worker tasks send the results
    back to the controller task, which writes them into its task report.
    If the query is an INSERT or REPLACE query, the worker tasks generate and
    publish new Druid segments to the provided datasource.