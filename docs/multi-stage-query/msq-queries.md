---
id: queries
title: Queries
---

> The multi-stage query architecture and its SQL-task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

You can submit queries to the SQL-task engine of the multi-stage query architecture through the **Query** view in the Druid console or through the API. The Druid console is a good place to start because you can preview a query before you run it. You can also experiment with many of the [context parameters](./msq-reference.md#context-parameters) through the UI. Once you're comfortable with submitting queries through the Druid console, explore [using the API to submit a query](./msq-api.md#submit-a-task-query).

If you encounter an issue after you submit a query, you can learn more about what an error means from the [limits](./msq-concepts.md#limits) and [errors](./msq-concepts.md#error-codes). 

Queries for the SQL-task engine involve three primary functions:

- EXTERN to query external data
- INSERT INTO ... SELECT to insert data, such as data from an external source
- REPLACE to replace existing datasources, partially or fully, with query results

For information about the syntax for queries, see [SQL syntax](./msq-reference.md#sql-syntax).

## Read external data

Task queries can access external data through the EXTERN function. When using EXTERN, keep in mind that the operator cannot split large files across different worker tasks. If you have fewer input files than worker tasks, you can increase query parallelism by splitting up your input files such that you have at least one input file per worker task.

You can use the EXTERN function anywhere a table is expected in the following form: `TABLE(EXTERN(...))`. You can use external data with SELECT, INSERT and REPLACE queries. 

The following query reads external data:

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

For more information about the syntax, see [EXTERN](./msq-reference.md#extern).

## Insert data

With the SQL-task engine, Druid can use the results of a task query to create a new datasource or to append to an existing datasource. Syntactically, there is no difference between the two. These operations use the INSERT INTO ... SELECT syntax.

All SELECT capabilities are available for INSERT queries. However, the SQL-task engine does not include all the existing SQL query features of Druid. See [Known issues](msq-release.md#known-issues) for a list of capabilities that aren't available.

The following example query inserts data from an external source into a table named `w000` and partitions it by day:

```sql
INSERT INTO w000
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

For more information about the syntax, see [INSERT](./msq-reference.md#insert).

## Replace data 

The syntax for REPLACE is similar to INSERT. All SELECT functionality is available for REPLACE queries.
Note that the SQL-task engine does not yet implement all native Druid query features.
For details, see [Known issues](./msq-release.md#known-issues).

When working with REPLACE queries, keep the following in mind:

- The intervals generated as a result of the OVERWRITE WHERE query must align with the granularity specified in the PARTITIONED BY clause.
- OVERWRITE WHERE queries only support the `__time` column.

For more information about the syntax, see [REPLACE](./msq-reference.md#replace).

The following examples show how to replace data in a table.

#### REPLACE all data

You can replace all the data in a table by using REPLACE INTO ... OVERWRITE ALL SELECT:

```sql
REPLACE INTO w000
OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
FROM TABLE(
  EXTERN(
    '{"type": "http", "uris": ["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type": "json"}',
    '[{"name": "timestamp", "type": "string"}, {"name": "page", "type": "string"}, {"name": "user", "type": "string"}]'
  )
)
PARTITIONED BY DAY
```

#### REPLACE some data

You can replace some of the data in a table by using REPLACE INTO ... OVERWRITE WHERE ... SELECT:

```sql
REPLACE INTO w000
OVERWRITE WHERE __time >= TIMESTAMP '2019-08-25' AND __time < TIMESTAMP '2019-08-28'
SELECT
  TIME_PARSE("timestamp") AS __time,
  "page",
  "user"
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

Changing query behavior can affect how your queries run or what your results look like. You can control how your queries behave by changing the following:

- [Read external data](#read-external-data)
- [Insert data](#insert-data)
- [Replace data](#replace-data)
    - [REPLACE all data](#replace-all-data)
    - [REPLACE some data](#replace-some-data)
- [Adjust query behavior](#adjust-query-behavior)
  - [Primary timestamp](#primary-timestamp)
  - [PARTITIONED BY](#partitioned-by)
  - [CLUSTERED BY](#clustered-by)
  - [GROUP BY](#group-by)
    - [Ingest-time aggregations](#ingest-time-aggregations)
    - [Multi-value dimensions](#multi-value-dimensions)
  - [Context parameters](#context-parameters)

### Primary timestamp

Druid tables always include a primary timestamp named `__time`, so your ingestion query should generally include a column named `__time`. 

The following formats are supported for `__time` in the source data:
- ISO 8601 with 'T' separator, such as "2000-01-01T01:02:03.456"
- Milliseconds since Unix epoch (00:00:00 UTC on January 1, 1970)

The `__time` column is used for time-based partitioning, such as `PARTITIONED BY DAY`.

If you use `PARTITIONED BY ALL` or `PARTITIONED BY ALL TIME`, time-based
partitioning is disabled. In these cases, your ingestion query doesn't need
to include a `__time` column. However, Druid still creates a `__time` column 
in your Druid table and sets all timestamps to 1970-01-01 00:00:00.

For more information, see [Primary timestamp](../ingestion/data-model.md#primary-timestamp).

### PARTITIONED BY

INSERT and REPLACE queries require the PARTITIONED BY clause, which determines how time-based partitioning is done. In Druid, data is split into segments, one or more per time chunk defined by the PARTITIONED BY granularity. A good general rule is to adjust the granularity so that each segment contains about five million rows. Choose a granularity based on your ingestion rate. For example, if you ingest a million rows per day, PARTITION BY DAY is good. If you ingest a million rows an hour, choose PARTITION BY HOUR instead.

Using the clause provides the following benefits:

- Better query performance due to time-based segment pruning, which removes segments from
   consideration when they do not contain any data for a query's time filter.
- More efficient data management, as data can be rewritten for each time partition individually
   rather than the whole table.

You can use the following arguments for PARTITIONED BY:

- Time unit: `HOUR`, `DAY`, `MONTH`, or `YEAR`. Equivalent to `FLOOR(__time TO TimeUnit)`.
- `TIME_FLOOR(__time, 'granularity_string')`, where granularity_string is an ISO8601 period like
  'PT1H'. The first argument must be `__time`.
- `FLOOR(__time TO TimeUnit)`, where `TimeUnit` is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first
  argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single
  time chunk. To use LIMIT or OFFSET at the outer level of your INSERT or REPLACE query, you must set PARTITIONED BY to ALL or ALL TIME.

You can use the following ISO8601 periods for `TIME_FLOOR`:

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

Data is first divided by the PARTITION BY clause. Data can be further split by the CLUSTERED BY clause. For example, suppose you ingest 100 M rows per hour and use `PARTITIONED BY HOUR` as your time partition. You then divide up the data further by adding `CLUSTERED BY hostName`. The result is segments of about 5 million rows, with like `hostNames` grouped within the same segment.

Using CLUSTERED BY has the following benefits:

- Lower storage footprint due to combining similar data into the same segments, which improves
   compressibility.
- Better query performance due to dimension-based segment pruning, which removes segments from
   consideration when they cannot possibly contain data matching a query's filter.

For dimension-based segment pruning to be effective, your queries should meet the following conditions:

- All CLUSTERED BY columns are single-valued string columns
- Use a REPLACE query for ingestion

Druid still clusters data during ingestion if these conditions aren't met but won't perform dimension-based segment pruning at query time. That means if you use an INSERT query for ingestion or have numeric columns or multi-valued string columns, dimension-based segment pruning doesn't occur at query time.

You can tell if dimension-based segment pruning is possible by using the `sys.segments` table to
inspect the `shard_spec` for the segments generated by an ingestion query. If they are of type
`range` or `single`, then dimension-based segment pruning is possible. Otherwise, it is not. The
shard spec type is also available in the **Segments** view under the **Partitioning**
column.

You can use the following filters for dimension-based segment pruning:

- Equality to string literals, like `x = 'foo'` or `x IN ('foo', 'bar')`.
- Comparison to string literals, like `x < 'foo'` or other comparisons involving `<`, `>`, `<=`, or `>=`.

This differs from multi-dimension range based partitioning in classic batch ingestion where both
string and numeric columns support Broker-level pruning. With SQL-based batch ingestion,
only string columns support Broker-level pruning.

It is okay to mix time partitioning with secondary partitioning. For example, you can
combine `PARTITIONED BY HOUR` with `CLUSTERED BY channel` to perform
time partitioning by hour and secondary partitioning by channel within each hour.

### GROUP BY

A query's GROUP BY clause determines how data is rolled up. The expressions in the GROUP BY clause become
dimensions, and aggregation functions become metrics.

#### Ingest-time aggregations

When performing rollup using aggregations, it is important to use aggregators
that return nonfinalized state. This allows you to perform further rollups
at query time. To achieve this, set `finalizeAggregations: false` in your
ingestion query context.

Check out the [INSERT with rollup example query](./msq-example-queries.md#insert-with-rollup) to see this feature in
action.

Druid needs information for aggregating measures of different segments while working with Pivot and compaction
tasks. For example, to aggregate `count("col") as example_measure`, Druid needs to sum the value of `example_measure`
across the segments. This information is stored inside the metadata of the segment. For the SQL-based ingestion, Druid only populates the
aggregator information of a column in the segment metadata when:

- The INSERT or REPLACE query has an outer GROUP BY clause.
- The following context parameters are set for the query context: `finalizeAggregations: false` and `groupByEnableMultiValueUnnesting: false`

The following table lists query-time aggregations for SQL-based ingestion:

|Query-time aggregation|Notes|
|----------------------|-----|
|SUM|Use unchanged at ingest time.|
|MIN|Use unchanged at ingest time.|
|MAX|Use unchanged at ingest time.|
|AVG|Use SUM and COUNT at ingest time. Switch to quotient of SUM at query time.|
|COUNT|Use unchanged at ingest time, but switch to SUM at query time.|
|COUNT(DISTINCT expr)|If approximate, use APPROX_COUNT_DISTINCT at ingest time.<br /><br />If exact, you cannot use an ingest-time aggregation. Instead, `expr` must be stored as-is. Add it to the SELECT and GROUP BY lists.|
|EARLIEST(expr)<br /><br />(numeric form)|Not supported.|
|EARLIEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|LATEST(expr)<br /><br />(numeric form)|Not supported.|
|LATEST(expr, maxBytes)<br /><br />(string form)|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_BUILTIN|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_HLL|Use unchanged at ingest time.|
|APPROX_COUNT_DISTINCT_DS_THETA|Use unchanged at ingest time.|
|APPROX_QUANTILE|Not supported. Deprecated; use APPROX_QUANTILE_DS instead.|
|APPROX_QUANTILE_DS|Use DS_QUANTILES_SKETCH at ingest time. Continue using APPROX_QUANTILE_DS at query time.|
|APPROX_QUANTILE_FIXED_BUCKETS|Not supported.|

#### Multi-value dimensions

By default, multi-value dimensions are not ingested as expected when rollup is enabled because the
GROUP BY operator unnests them instead of leaving them as arrays. This is [standard behavior](../querying/sql-data-types.md#multi-value-strings) for GROUP BY but it is generally not desirable behavior for ingestion.

To address this:

- When using GROUP BY with data from EXTERN, wrap any string type fields from EXTERN that may be
  multi-valued in `MV_TO_ARRAY`.
- Set `groupByEnableMultiValueUnnesting: false` in your query context to ensure that all multi-value
  strings are properly converted to arrays using `MV_TO_ARRAY`. If any strings aren't
  wrapped in `MV_TO_ARRAY`, the query reports an error that includes the message "Encountered
  multi-value dimension x that cannot be processed with groupByEnableMultiValueUnnesting set to false."

For an example, see [INSERT with rollup example query](./msq-example-queries.md#insert-with-rollup).

### Context parameters

Context parameters can control things such as how many tasks get launched or what happens if there's a malformed record.

For a full list of context parameters and how they affect a query, see [Context parameters](./msq-reference.md#context-parameters).
