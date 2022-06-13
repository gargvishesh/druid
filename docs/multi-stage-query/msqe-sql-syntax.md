---
id: msqe-syntax
title: SQL syntax 
---

## MSQE query statements

Queries for MSQE involve 3 primary functions:

- EXTERN to query external data
- INSERT INTO ... SELECT to insert data, such as data from an external source
- REPLACE to replace existing datasources, partially or fully, with query results

### EXTERN

MSQE queries can access external data through the `EXTERN` function. It can appear in the form `TABLE(EXTERN(...))` anywhere a table is expected. You can use external data with SELECT, INSERT and REPLACE queries. 

The `EXTERN` function uses three parameters:

1.  Any [Druid input source](https://docs.imply.io/latest/druid/ingestion/native-batch.html#input-sources) as a JSON-encoded string.
2.  Any [Druid input
    format](https://docs.imply.io/latest/druid/ingestion/data-formats.html#input-format)
    as a JSON-encoded string.

3.  A row signature, as a JSON-encoded array of column descriptors. Each
    column descriptor must have a `name` and a `type`. The type can be
    `string`, `long`, `double`, or `float`. This row signature is
    used to map the external data into the SQL layer.

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

### INSERT

With MSQE, Druid can use the results of a query to create a new datasource
or to append or replace data in an existing datasource. Syntactically, there is no difference between the two. These operations  use the INSERT INTO ... SELECT syntax.

All SELECT capabilities are available for INSERT queries. However, MSQE does not include all native Druid query features. See [Known issues](msqe-release.md#known-issues) for a list of capabilities that aren't available.

An INSERT query consists of the following parts:

1. Optional [context parameters](./msqe-reference.md#context-variables).
2. An `INSERT INTO <dataSource>` clause at the start of your query, such as `INSERT INTO w000` in the example that follows.
3. A clause for the data you want to insert, `SELECT...FROM TABLE...` in the example.
4. A [PARTITIONED BY](#partitioned-by) clause for your INSERT statement. For example, use `PARTITIONED BY DAY` for daily partitioning or `PARTITIONED BY ALL TIME` to skip time partitioning completely.
5. An optional [CLUSTERED BY](#clustered-by) clause

The following query inserts data from an external source into a table named `w000` and partitions it by day:

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

This example table is used in the other examples in this section and others.

### REPLACE 

The syntax for REPLACE syntax is similar to INSERT. All SELECT functionality is available for REPLACE queries. However, keep in mind that MSQE does not yet implement all native Druid query features. See
[Known issues](./msqe-release.md#known-issues) for a list of functionality that is not available.


When working with REPLACE queries, keep the following in mind:

- The intervals generated as a result of the `OVERWRITE WHERE` query must align with the granularity specified in the PARTITIONED BY clause.
- Only the `__time` column is supported in OVERWRITE WHERE queries.


An REPLACE query consists of the following parts:

1. Optional [context parameters](./msqe-reference.md#context-variables).
2. A `REPLACE INTO <dataSource>` clause at the start of your query, such as `REPLACE INTO w000` in the examples that follow.
3. An OVERWRITE clause after the datasource, either OVERWRITE ALL or OVERWRITE WHERE ...:

  - OVERWRITE ALL replaces the entire existing datasource with the results of the query.

  - OVERWRITE WHERE drops the time segments that match the condition you set. Conditions are based on the `__time` column and use the format `__time [< > = <= >=] TIMESTAMP`. Use them with AND, OR and NOT between them. `BETWEEN TIMESTAMP AND TIMESTAMP` (inclusive of the timestamps specified) is also a supported condition for `OVERWRITE WHERE`. For an example, see [REPLACE INTO ... OVERWRITE WHERE ... SELECT](#replace-into--overwrite-where--select).
3. A clause for the actual data you want to use for the replacement.
4. A [PARTITIONED BY](#partitioned-by) clause to your INSERT statement. For example, use PARTITIONED BY DAY for daily partitioning, or PARTITIONED BY ALL TIME to skip time partitioning completely.
5. An optional [CLUSTERED BY](#clustered-by) clause.


#### REPLACE INTO ... OVERWRITE ALL SELECT

```sql
REPLACE INTO w000
OVERWRITE ALL WHERE __time >= TIMESTAMP '2019-08-25 02:00:00' AND __time < TIMESTAMP '2019-08-25 03:00:00'
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

#### REPLACE INTO ... OVERWRITE WHERE ... SELECT

```sql
REPLACE INTO w000
OVERWRITE WHERE __time >= TIMESTAMP '2019-08-25' AND __time < TIMESTAMP '2019-08-28'
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

## Adjust query behavior

You can control how your queries behave by changing the following:

- Primary timestamp
- PARTITIONED BY
- CLUSTERED BY
- Rollup
- Context parameters

### Primary timestamp

Druid tables always include a primary timestamp named `__time`, so your ingestion query should generally include a column named `__time`. The column is used for time-based partitioning, such as
`PARTITIONED BY DAY`.

If you use `PARTITIONED BY ALL` or `PARTITIONED BY ALL TIME`, time-based
partitioning is disabled. In these cases, your ingestion query doesn't need
to include a `__time` column. However, Druid still creates a `__time` column 
in your Druid table and sets all timestamps to 1970-01-01 00:00:00.

For more information, see [Primary timestamp](../ingestion/data-model.md#primary-timestamp).

### PARTITIONED BY

INSERT and REPLACE queries require the PARTITIONED BY clause, which determines how time-based partitioning is done. In Druid, data is split into segments, one per time chunk defined by the PARTITIONED BY granularity. A good general rule is to adjust the granularity so that each segment contains about five million rows. Choose a grain based on your ingestion rate. For example, if you ingest a million rows per day, PARTITION BY DAY is good. If you ingest a million rows an hour, choose PARTITION BY HOUR instead.

Using the clause provides the following benefits:

- Better query performance due to time-based segment pruning, which removes segments from
   consideration when they do not contain any data for a query's time filter.
- More efficient data management, as data can be rewritten for each time partition individually
   rather than the whole table.

You can use the following arguments for PARTITIONED BY:

- Time unit: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is an ISO8601 period like
  'PT1H'. The first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where TimeUnit is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first
  argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single
  time chunk. To use LIMIT or OFFSET at the outer level of your INSERT or REPLACE query, you must set PARTITIONED BY to `ALL` or `ALL TIME`.

MSQE supports the ISO8601 periods for `TIME_FLOOR`:

- PT1S
- PT1M
- PT5M
- PT10M
- PT15M
- PT30M
- PT1H
- PT6H
- P1D
- P1W
- P1M
- P3M
- P1Y


### CLUSTERED BY

Data is first divided by the PARTITION BY clause. Data can be further split by the CLUSTERED BY clause. For example, suppose you ingest 100M rows per hour and use `PARTITIONED BY HOUR` as your time partition. You then divide up the data further by adding `CLUSTERED BY hostName`. The result will be segments of about 5 million rows, with like hostNames grouped within the same segment.

Using CLUSTERED BY has the following benefits:

- Lower storage footprint due to combining similar data into the same segments, which improves
   compressibility.
- Better query performance due to dimension-based segment pruning, which removes segments from
   consideration when they cannot possibly contain data matching a query's filter.

For dimension-based segment pruning to be effective, all CLUSTERED BY columns must be single-valued
string columns and the [`sqlReplaceTimeChunks`](./msqe-reference.md#context-variables) parameter must be provided as part of
the INSERT query. If the CLUSTERED BY list contains any numeric columns or multi-valued string
columns or if `sqlReplaceTimeChunks` is not provided, Druid still clusters data during
ingestion but won't perform dimension-based segment pruning at query time.

You can tell if dimension-based segment pruning is possible by using the `sys.segments` table to
inspect the `shard_spec` for the segments that are generated by an ingestion query. If they are of type
`range` or `single`, then dimension-based segment pruning is possible. Otherwise, it is not. The
shard spec type is also available in the **Segments** view under the **Partitioning**
column.

You can use the following filters for dimension-based segment pruning:

- Equality to string literals, like `x = 'foo'` or `x IN ('foo', 'bar')`.
- Comparison to string literals, like `x < 'foo'` or other comparisons involving `<`, `>`, `<=`, or `>=`.

This differs from multi-dimension range based partitioning in classic batch ingestion where both
string and numeric columns support Broker-level pruning. With MSQE ingestion,
only string columns support Broker-level pruning.

It is okay to mix time partitioning with secondary partitioning. For example, you can
combine `PARTITIONED BY HOUR` with `CLUSTERED BY channel` to perform
time partitioning by hour and secondary partitioning by channel within each hour.

### Rollup

Rollup is determined by the query's GROUP BY clause. The expressions in the GROUP BY clause become
dimensions, and aggregation functions become metrics.

#### Ingest-time aggregations

When performing rollup using aggregations, it is important to use aggregators
that return _nonfinalized_ state. This allows you to perform further rollups
at query time. To achieve this, set `msqFinalizeAggregations: false` in your
ingestion query context and refer to the following table for any additional
modifications needed.

Check out the [INSERT with rollup example query](./msqe-quickstart.md#insert-with-rollup) to see this feature in
action.

Druid needs information for aggregating measures of different segments while working with Pivot and compaction
tasks. For example, to aggregate `count("col") as example_measure`, Druid needs to sum the value of `example_measure`
across the segments. This information is stored inside the metadata of the segment. For MSQE, Druid only populates the
aggregator information of a column in the segment metadata when:

- The INSERT or REPLACE query has an outer GROUP BY clause.
- The following parameters are set for the query context: `msqFinalizeAggregations: false` and `groupByEnableMultiValueUnnesting: false`

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

By default, multi-value dimensions are not ingested as expected when rollup is enabled because the
GROUP BY operator unnests them instead of leaving them as arrays. This is [standard behavior](../querying/sql-data-types.md#multi-value-strings) for GROUP BY but is generally not desirable behavior for ingestion.

To address this:

- When using GROUP BY with data from EXTERN, wrap any `string`-typed fields from EXTERN that may be
  multi-valued in `MV_TO_ARRAY`.
- Set `groupByEnableMultiValueUnnesting: false` in your query context to ensure that all multi-value
  strings are properly converted to arrays using `MV_TO_ARRAY`. If any strings aren't
  wrapped in `MV_TO_ARRAY`, the query reports an error that includes the message "Encountered
  multi-value dimension x that cannot be processed with groupByEnableMultiValueUnnesting set to false."

For an example, see [INSERT with rollup example query](./msqe-quickstart.md#insert-with-rollup).