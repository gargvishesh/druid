---
id: queries
title: Queries
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

You can submit queries to the Multi-Stage Query (MSQ) Framework through the **Query** view in the Druid console or the API. The UI is a good place to start because you can preview a query before you run it. You can also experiment with many of the [context parameters](./msq-reference.md#context-parameters through the UI. Once you're comfortable with a query, explore [using the API to submit a query](./msq-api.md#submit-a-task-query).

If you encounter an issue after you submit a query, you can learn more about what an error means from [this table](./msq-reference.md#context-parameters). 

Queries for MSQ involve 3 primary functions:

- EXTERN to query external data
- INSERT INTO ... SELECT to insert data, such as data from an external source
- REPLACE to replace existing datasources, partially or fully, with query results

For information about the syntax for queries, see [SQL syntax](./msq-reference.md#sql-syntax).

## Read external data

Task queries can access external data through the EXTERN function. When using EXTERN, keep in mind that the operator cannot split large files across different worker tasks. If you have fewer input files than worker tasks, you can increase query parallelism by splitting up your input files such that you have at least one input file per worker task.

The EXTERN function can be used anywhere a table is expected in the following form: `TABLE(EXTERN(...))`.  You can use external data with SELECT, INSERT and REPLACE queries. 

The following query reads external data 

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

With MSQ, Druid can use the results of a task query to create a new datasource
or to append to an existing datasource. Syntactically, there is no difference between the two. These operations  use the INSERT INTO ... SELECT syntax.

All SELECT capabilities are available for INSERT queries. However, MSQ does not include all native Druid query features. See [Known issues](msq-release.md#known-issues) for a list of capabilities that aren't available.

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

The syntax for REPLACE syntax is similar to INSERT. All SELECT functionality is available for REPLACE queries. However, keep in mind that MSQ does not yet implement all native Druid query features. See
[Known issues](./msq-release.md#known-issues) for a list of functionality that is not available.

When working with REPLACE queries, keep the following in mind:

- The intervals generated as a result of the `OVERWRITE WHERE` query must align with the granularity specified in the PARTITIONED BY clause.
- Only the `__time` column is supported in OVERWRITE WHERE queries.

For more information about the syntax, see [REPLACE](./msq-reference.md#replace).

The following examples show you how to either replace all data in a table or only some data.

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

You can replace some of the data in a table using REPLACE INTO ... OVERWRITE WHERE ... SELECT:

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

- Primary timestamp
- PARTITIONED BY
- CLUSTERED BY
- GROUP BY
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
- `FLOOR(__time TO TimeUnit)`, where TimeUnit is any unit supported by the [FLOOR function](../querying/sql-scalar.md#date-and-time-functions). The first
  argument must be `__time`.
- `ALL` or `ALL TIME`, which effectively disables time partitioning by placing all data in a single
  time chunk. To use LIMIT or OFFSET at the outer level of your INSERT or REPLACE query, you must set PARTITIONED BY to `ALL` or `ALL TIME`.

MSQ supports the ISO8601 periods for `TIME_FLOOR`:

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

For dimension-based segment pruning to be effective, your queries should meet the following conditions:

- All CLUSTERED BY columns are single-valued string columns
- Use a REPLACE query for ingestion

Druid still clusters data during ingestion if these conditions aren't met but won't perform dimension-based segment pruning at query time. That means if you use an INSERT query for ingestion or have numeric columns or multi-valued string columns, dimension-based segment pruning doesn't occur at query time.

You can tell if dimension-based segment pruning is possible by using the `sys.segments` table to
inspect the `shard_spec` for the segments that are generated by an ingestion query. If they are of type
`range` or `single`, then dimension-based segment pruning is possible. Otherwise, it is not. The
shard spec type is also available in the **Segments** view under the **Partitioning**
column.

You can use the following filters for dimension-based segment pruning:

- Equality to string literals, like `x = 'foo'` or `x IN ('foo', 'bar')`.
- Comparison to string literals, like `x < 'foo'` or other comparisons involving `<`, `>`, `<=`, or `>=`.

This differs from multi-dimension range based partitioning in classic batch ingestion where both
string and numeric columns support Broker-level pruning. With MSQ ingestion,
only string columns support Broker-level pruning.

It is okay to mix time partitioning with secondary partitioning. For example, you can
combine `PARTITIONED BY HOUR` with `CLUSTERED BY channel` to perform
time partitioning by hour and secondary partitioning by channel within each hour.

### GROUP BY

A query's GROUP BY clause determines how data is rolled up. The expressions in the GROUP BY clause become
dimensions, and aggregation functions become metrics.

#### Ingest-time aggregations

When performing rollup using aggregations, it is important to use aggregators
that return _nonfinalized_ state. This allows you to perform further rollups
at query time. To achieve this, set `msqFinalizeAggregations: false` in your
ingestion query context and refer to the following table for any additional
modifications needed.

Check out the [INSERT with rollup example query](./msq-queries.md#insert-with-rollup) to see this feature in
action.

Druid needs information for aggregating measures of different segments while working with Pivot and compaction
tasks. For example, to aggregate `count("col") as example_measure`, Druid needs to sum the value of `example_measure`
across the segments. This information is stored inside the metadata of the segment. For MSQ, Druid only populates the
aggregator information of a column in the segment metadata when:

- The INSERT or REPLACE query has an outer GROUP BY clause.
- The following context parameters are set for the query context: `msqFinalizeAggregations: false` and `groupByEnableMultiValueUnnesting: false`

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

For an example, see [INSERT with rollup example query](./msq-queries.md#insert-with-rollup).

### Context parameters

Context parameters can control things such as how many tasks get launched or what happens if MSQ encounters a malformed record.

For a full list of context parameters and how they affect a query, see [Context parameters](./msq-reference.md#context-parameters).

## Query examples

These example queries show you some of the other things you can do to modify your MSQ queries depending on your use case. You can copy the example queries into the **Query** UI and run them.

### INSERT with no rollup

This example inserts data into a table named `w000` without performing any data rollup:

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w000
SELECT
  TIME_PARSE("timestamp") AS __time,
  isRobot,
  channel,
  flags,
  isUnpatrolled,
  page,
  diffUrl,
  added,
  comment,
  commentLength,
  isNew,
  isMinor,
  delta,
  isAnonymous,
  user,
  deltaBucket,
  deleted,
  namespace,
  cityName,
  countryName,
  regionIsoCode,
  metroCode,
  countryIsoCode,
  regionName
FROM TABLE(
    EXTERN(
      '{"type":"http","uris":["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
      '{"type":"json"}',
      '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"timestamp","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"long"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
    )
  )
PARTITIONED BY HOUR
CLUSTERED BY channel
```

</details>

### INSERT with rollup

This example inserts data into a table named `kttm_data` and performs data rollup. This example implements the recommendations described in [multi-value dimensions](./msq-queries.md#multi-value-dimensions).

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
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

</details>

### INSERT for reindexing an existing datasource

This example rolls up data from a table named `w000` and inserts the result into `w002`.

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
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

</details>


### INSERT with JOIN

This example inserts data into a table named `w003` and joins data from two sources:

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

INSERT INTO w003
WITH
wikidata AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
    '{"type":"json"}',
    '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"timestamp","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"long"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
  )
)),
countries AS (SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))
SELECT
  TIME_PARSE("timestamp") AS __time,
  isRobot,
  channel,
  flags,
  isUnpatrolled,
  page,
  diffUrl,
  added,
  comment,
  commentLength,
  isNew,
  isMinor,
  delta,
  isAnonymous,
  user,
  deltaBucket,
  deleted,
  namespace,
  cityName,
  countryName,
  regionIsoCode,
  metroCode,
  countryIsoCode,
  countries.Capital AS countryCapital,
  regionName
FROM wikidata
LEFT JOIN countries ON wikidata.countryIsoCode = countries.ISO2
PARTITIONED BY HOUR
```

</details>

### REPLACE an entire datasource

This example replaces the entire datasource used in the table `w007` with the new query data while dropping the old data:

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

REPLACE INTO w007
OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS __time,
  isRobot,
  channel,
  flags,
  isUnpatrolled,
  page,
  diffUrl,
  added,
  comment,
  commentLength,
  isNew,
  isMinor,
  delta,
  isAnonymous,
  user,
  deltaBucket,
  deleted,
  namespace,
  cityName,
  countryName,
  regionIsoCode,
  metroCode,
  countryIsoCode,
  regionName
FROM TABLE(
    EXTERN(
      '{"type":"http","uris":["https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json"]}',
      '{"type":"json"}',
      '[{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"timestamp","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"long"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
    )
  )
PARTITIONED BY HOUR
CLUSTERED BY channel
```

</details>

### REPLACE for replacing a specific time segment

This example replaces certain segments in a datasource with the new query data while dropping old segments:

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

REPLACE INTO w007
OVERWRITE WHERE __time >= TIMESTAMP '2019-08-25 02:00:00' AND __time < TIMESTAMP '2019-08-25 03:00:00'
SELECT
  FLOOR(__time TO MINUTE) AS __time,
  channel,
  countryIsoCode,
  countryName,
  regionIsoCode,
  regionName,
  page
FROM w007
WHERE __time >= TIMESTAMP '2019-08-25 02:00:00' AND __time < TIMESTAMP '2019-08-25 03:00:00' AND countryName = "Canada"
PARTITIONED BY HOUR
CLUSTERED BY page
```

</details>

### REPLACE for reindexing an existing datasource into itself

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false

REPLACE INTO w000
OVERWRITE ALL
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

</details>

### SELECT with EXTERN and JOIN


<details><summary>Show the query</summary>


```sql
--:context msqFinalizeAggregations: false
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

</details>