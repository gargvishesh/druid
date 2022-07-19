---
id: reference
title: Reference
---

## Context parameters

In addition to the Druid SQL [context parameters](../querying/sql-query-context.md), Multi-Stage Query (MSQ) supports context parameters that are specific to it. 

You provide context parameters alongside your queries to customize the behavior of the query. The way you provide the parameters depends on how you submit your query:

- **Druid console**: Add your parameters before your query like so:

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
| msqMaxNumTasks | (SELECT, INSERT, or REPLACE)<br /><br />MSQ executes queries using the indexing service, that is using the Overlord + MiddleManager. This property specifies the maximum total number of tasks to launch, including the controller task.<br /><br />The lowest possible value for this setting is 2: one controller and one worker.<br /><br />All tasks must be able to launch simultaneously. If they cannot, the query will return a `TaskStartTimeout` error code after about 10 minutes.<br /><br />May also be provided as `msqNumTasks`. If both are present, `msqMaxNumTasks` takes priority.| 2 |
| msqTaskAssignment | (SELECT, INSERT, or REPLACE)<br /><br />Determines how the number of tasks is chosen.<br /><br />`max`: use as many tasks as possible, up to the maximum `msqMaxNumTasks`.<br /><br />`auto`: use as few tasks as possible without exceeding 10 Gib or 10,000 files per task. Please review the [limitations](./msq-release.md#general-query-execution) of `auto` mode before using it.| `max` |
| msqFinalizeAggregations | (SELECT, INSERT, or REPLACE)<br /><br />Whether Druid will finalize the results of complex aggregations that directly appear in query results.<br /><br />If false, Druid returns the aggregation's intermediate type rather than finalized type. This parameter is useful during ingestion, where it enables storing sketches directly in Druid tables. For more information about aggregations, see [SQL aggregation functions](../querying/sql-aggregations.md). | true |
| msqRowsInMemory | (INSERT or REPLACE)<br /><br />Maximum number of rows to store in memory at once before flushing to disk during the segment generation process. Ignored for non-INSERT queries.<br /><br />In most cases, you should stick to the default. It may be necessary to override this if you run into one of the current [known issues around memory usage](./msq-release.md#memory-usage)</a>. | 100,000 |
| msqSegmentSortOrder | (INSERT or REPLACE)<br /><br />Normally, Druid sorts rows in individual segments using `__time` first, then the [CLUSTERED BY](./msq-queries.md#clustered-by) clause. When `msqSegmentSortOrder` is set, Druid sorts rows in segments using this column list first, followed by the CLUSTERED BY order.<br /><br />The column list can be provided as comma-separated values or as a JSON array in string form. If your query includes `__time`, then this list must begin with `__time`.<br /><br />For example: consider an INSERT query that uses `CLUSTERED BY country` and has `msqSegmentSortOrder` set to `__time,city`. Within each time chunk, Druid assigns rows to segments based on `country`, and then within each of those segments, Druid sorts those rows by `__time` first, then `city`, then `country`. | empty list |
| maxParseExceptions| (SELECT, INSERT, or REPLACE)<br /><br />Maximum number of parse exceptions that are ignored while executing the query before it stops with `TooManyWarningsFault`. To ignore all the parse exceptions, set the value to -1.<br /><br />| 0 |
| msqRowsPerSegment        | (INSERT or REPLACE)<br /><br />Number of rows per segment to target. The actual number of rows per segment may be somewhat higher or lower than this number. In most cases, you should stick to the default. For general information about sizing rows per segment, see [Segment Size Optimization](../operations/segment-optimization.md). | 3,000,000 |
| msqDurableShuffleStorage | (SELECT, INSERT, or REPLACE) <br /><br />Whether to use durable storage for shuffle mesh. To use this feature, durable storage must be configured at the server level using `druid.msq.intermediate.storage.enable=true` and its [associated properties](./msq-advanced-configs.md#durable-storage-for-mesh-shuffle). If these properties are not configured, any query with the context variable `msqDurableShuffleStorage=true` fails with a configuration error. <br /><br /> | false |
|`sqlTimeZone`|Sets the time zone for this connection, which will affect how time functions and timestamp literals behave. Should be a time zone name like "America/Los_Angeles" or offset like "-08:00".|druid.sql.planner.sqlTimeZone on the Broker (default: UTC)|
|`useApproximateCountDistinct`|Whether to use an approximate cardinality algorithm for `COUNT(DISTINCT foo)`.|druid.sql.planner.useApproximateCountDistinct on the Broker (default: true)|

## Error codes

MSQ errors are meant to be human readable and provide you with enough information 

## SQL syntax

MSQ has three primary SQL functions: 

- EXTERN
- INSERT
- REPLACE

For information about using these and examples, see [MSQ queries](./msq-queries.md). For information about adjusting the shape of your data, see [Adjust query behavior](./msq-queries.md#adjust-query-behavior).

### EXTERN

The EXTERN function is used to read external data and uses the following format:

```sql
SELECT
 <column>
FROM TABLE(
  EXTERN(
    '<Druid input source>',
    '<Druid input format>',
    '<row signature>'
  )
)
```

The EXTERN function uses three parameters:

1.  Any [Druid input source](https://docs.imply.io/latest/druid/ingestion/native-batch.html#input-sources) as a JSON-encoded string.
2.  Any [Druid input
    format](https://docs.imply.io/latest/druid/ingestion/data-formats.html#input-format)
    as a JSON-encoded string.
3.  A row signature, as a JSON-encoded array of column descriptors. Each
    column descriptor must have a `name` and a `type`. The type can be
    `string`, `long`, `double`, or `float`. This row signature is
    used to map the external data into the SQL layer.

### INSERT

```sql
INSERT INTO <table name>
SELECT
  <column>
FROM <table>
PARTITIONED BY <time frame>
```

An INSERT query consists of the following parts:

1. Optional [context parameters](./msq-reference.md#context-parameters).
2. An `INSERT INTO <dataSource>` clause at the start of your query, such as `INSERT INTO w000`.
3. A clause for the data you want to insert, such as`SELECT...FROM TABLE...`. You can use EXTERN to reference an external tables using the following format: ``TABLE(EXTERN(...))`.
4. A [PARTITIONED BY](./msq-queries.md#partitioned-by) clause for your INSERT statement. For example, use `PARTITIONED BY DAY` for daily partitioning or `PARTITIONED BY ALL TIME` to skip time partitioning completely.
5. An optional [CLUSTERED BY](./msq-queries.md#clustered-by) clause.

### REPLACE

#### REPLACE all data

```sql
REPLACE INTO <target table>
OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS __time,
  *
FROM <source table>

PARTITIONED BY <time>
```

#### REPLACE specific data

```sql
REPLACE INTO <target table>
OVERWRITE WHERE __time >= TIMESTAMP '<lower bound>' AND __time < TIMESTAMP '<upper bound>'
SELECT
  TIME_PARSE("timestamp") AS __time,
  *
FROM <source table>

PARTITIONED BY <time>
```

A REPLACE query consists of the following parts:

1. Optional [context parameters](./msq-reference.md#context-parameters).
2. A `REPLACE INTO <dataSource>` clause at the start of your query, such as `REPLACE INTO w000`.
3. An OVERWRITE clause after the datasource, either OVERWRITE ALL or OVERWRITE WHERE ...:

  - OVERWRITE ALL replaces the entire existing datasource with the results of the query.

  - OVERWRITE WHERE drops the time segments that match the condition you set. Conditions are based on the `__time` column and use the format `__time [< > = <= >=] TIMESTAMP`. Use them with AND, OR and NOT between them. `BETWEEN TIMESTAMP AND TIMESTAMP` (inclusive of the timestamps specified). For an example, see [REPLACE INTO ... OVERWRITE WHERE ... SELECT](./msq-queries.md#replace-some-data).
4. A clause for the actual data you want to use for the replacement.
5. A [PARTITIONED BY](./msq-queries.md#partitioned-by) clause to your REPLACE statement. For example, use PARTITIONED BY DAY for daily partitioning, or PARTITIONED BY ALL TIME to skip time partitioning completely.
6. An optional [CLUSTERED BY](./msq-queries.md#clustered-by) clause.