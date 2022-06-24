---
id: msqe-quickstart
title: Quickstart
---

> The Multi-Stage Query Engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

Before you start, make sure you've [enabled the Multi-Stage Query Engine (MSQE)](msqe-setup.md).

## Enhanced Query view

MSQE uses the Enhanced Query view, which provides you with a UI to edit and use SQL queries. If MSQE is enabled, the enhanced Query view is available automatically. No additional configuration is required.

The following screenshot shows you a populated enhanced Query view along with a description of its parts: 

<details open><summary>Show the screenshot</summary>

![Annotated multi-stage query view](../assets/multi-stage-query/ui-annotated.png)

1. The enhanced Query view is where you interact with MSQE. You can access the original Query view by navigating to `#query` in the URL. 

2. The resources view shows the available schemas, datasources, and columns just like in the original Query view.

3. Query tabs allow you to save, run, and manage multiple queries at once.

4. The **Connect external data** wizard generates a basic `EXTERN` helper query by sampling your external datasource. You can then review and modify the query to suit your needs.

5. The **Wrench icon** contains several tools that can help you build SQL queries, including a tool that converts your ingestion spec to SQL.

6. The **Work history** panel shows you the previous queries executed by all users in the cluster.
It is equivalent to the task view in the Ingestion tab with the filter of `
type=’query_controller’`.
You can show or hide this panel by selecting the **Wrench icon** (5).

7. You can click on each query entry in **Work history** to attach to that query and track its progress.
Additionally, you can see results, query stats, and the query.

8. The query helpers let you define notebook-style queries that can be referenced from the main query as if they were defined as WITH clauses. You can refer to this extern 
by name anywhere in the SQL query where you can use a table.
They are essentially UI driven views that only exist within the given query tab.

9. Context comments can be inserted anywhere in the query to set context parameters. These comments are parsed out of the query text and added to the context object in the API payload. For a full list, see [Context parameters](./msqe-api.md#context-variables).

10.  The `Run` button’s more menu (`...`) lets you export the data as well as define the context for the query including the parallelism for the query.

11.  The `Preview` button runs the query without the INSERT into clause and with an added LIMIT to the main query and to all helper queries. Use it to see the general shape of the data before you commit to inserting it.
The added LIMITs make the query run faster but could cause incomplete results.

12. The query timer indicates how long the query has been running.

13.   The `(cancel)` link cancels the currently running query.

14. The main progress bar shows the overall progress of the query.
The progress is computed from the various counters in the live reports (16).

15. The **Current stage** progress bar shows the progress for the current query stage.
If several stages are executing concurrently, it shows the information for the earliest executing stage.

16. The live query reports show detailed information of all the stages (past, present, and future). The live reports are shown while the query is running. 
After a query finishes, reports are available by clicking on the query time indicator or from **Work history** (6).

17. Each stage of the live query reports can be expanded to show per worker and per partition statistics.

</details>

## Create and run queries

The following section takes you through running a series of queries that reference externally hosted data. The best way to start running these queries is with the enhanced **Query** view in the Druid web console. 

### Examine and load external data

The following example uses EXTERN to query a JSON file located at https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json. 

Although you can manually create a query in the UI, you can use Druid to generate a base query for you that you can modify to meet your requirements.

To generate a query from external data, do the following:

1. Select **Connect external data**. Since you're doing a new ingestion, choose **Connect external data in a new tab**.
2. On the **Select input type** screen, choose **HTTP(s)** and add the following value to the URI field: `https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json`. Leave the username and password blank.
3. Click **Connect data**.
4. On the **Parse** screen, you can perform a few actions before you load the data into Druid:
   - Expand a row to see what data it corresponds to from the source.
   - Customize how the data is handled by selecting the **Input format** and its related options, such as adding **JSON parser features** for JSON files.
5. When you're ready, click **Done**. You're returned to a **Query** tab in the console where you can see the query that the Druid console generated:

   - The query includes context variables for MSQE.  The syntax is unique to the Console: `--: context {key}: {value}`. When submitting queries to Druid directly, set the context variables in the context section of the SQL query object. For more information about context parameters, see [Context variables](./msqe-api.md#context-variables).
   - For information about what the different parts of this query are, see [MSQE SQL syntax](./msqe-sql-syntax.md).
   - The query inserts the data from the external source into a table named `wikipedia-2016-06-27-sampled`:

   <details><summary>Show the query</summary>

   ```sql
   --:context msqFinalizeAggregations: false
   --:context groupByEnableMultiValueUnnesting: false
   INSERT INTO "wikipedia-2016-06-27-sampled"
   SELECT
     isRobot,
     channel,
     "timestamp",
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
   FROM "wikipedia-2016-06-27-sampled"
   PARTITIONED BY ALL
   ```
   </details>

6. Review and modify the query to meet your needs. For example, you can change the table name or the partitioning to `PARTITIONED BY DAY`to specify day-based segment granularity. Note that if you want to partition by something other than ALL, you need to include `TIME_PARSE("timestamp") AS __time` in your SELECT statement:
   
   ```sql
   ...
   SELECT
     TIME_PARSE("timestamp") AS __time,
   ...
   ...
    PARTITIONED BY DAY
    ```

7. Optionally, select **Preview** to review the data before you ingest it. A preview runs the query without the INSERT into clause and with an added LIMIT to the main query and to all helper queries. You can see the general shape of the data before you commit to inserting it. The  LIMITs make the query run faster but could cause incomplete results.
8. Run your query. The query returns information including the number of rows inserted into the table named `wikipedia-2016-06-27-sampled` and how long the query took.

### Query the data

The data that you loaded into `wikipedia-2016-06-27-sampled` is queryable after the ingestion completes. You can analyze the data in the table to do things like produce a list of top channels:

```sql
SELECT
  channel,
  COUNT(*)
FROM "wikipedia-2016-06-27-sampled"
GROUP BY channel
ORDER BY COUNT(*) DESC
```

With the EXTERN function, you could also run the same query on the external data directly:

<details><summary>Show the query</summary>

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

</details>

### Convert a JSON ingestion spec

If you're already ingesting data with Druid's core query engine, you can use the Druid console to help you convert the ingestion spec to a SQL query.

In the **Query** view, do the following:

1. Select the **Wrench icon** **>** **Convert ingestion spec to SQL**.
2. Provide your ingestion spec. You can use this sample ingestion spec if you don't have one:

   <details><summary>Show the spec</summary>
   This spec uses data hosted at `https://static.imply.io/data/wikipedia.json.gz` and loads it into a table named `wikipedia`.
   
   ```json
   {
     "type": "index_parallel",
     "spec": {
       "ioConfig": {
         "type": "index_parallel",
         "inputSource": {
           "type": "http",
           "uris": [
             "https://static.imply.io/data/wikipedia.json.gz"
           ]
         },
         "inputFormat": {
           "type": "json"
         }
       },
       "tuningConfig": {
         "type": "index_parallel",
         "partitionsSpec": {
           "type": "dynamic"
         }
       },
       "dataSchema": {
         "dataSource": "wikipedia",
         "timestampSpec": {
           "column": "timestamp",
           "format": "iso"
         },
         "dimensionsSpec": {
           "dimensions": [
             "isRobot",
             "channel",
             "flags",
             "isUnpatrolled",
             "page",
             "diffUrl",
             {
               "type": "long",
               "name": "added"
             },
             "comment",
             {
               "type": "long",
               "name": "commentLength"
             },
             "isNew",
             "isMinor",
             {
               "type": "long",
               "name": "delta"
             },
             "isAnonymous",
             "user",
             {
               "type": "long",
               "name": "deltaBucket"
             },
             {
               "type": "long",
               "name": "deleted"
             },
             "namespace",
             "cityName",
             "countryName",
             "regionIsoCode",
             "metroCode",
             "countryIsoCode",
             "regionName"
           ]
         },
         "granularitySpec": {
           "queryGranularity": "none",
           "rollup": false,
           "segmentGranularity": "day"
         }
       }
     }
   }
   ```
   
   </details>

3. Submit the spec. The Druid console uses the JSON-based ingestion spec to generate a SQL query that you can use instead.
   
   <details><summary>Show the query</summary>
   ```sql
   -- This SQL query was auto generated from an ingestion spec
   --:context msqFinalizeAggregations: false
   --:context groupByEnableMultiValueUnnesting: false
   INSERT INTO wikipedia
   WITH ioConfigExtern AS (SELECT * FROM TABLE(
     EXTERN(
       '{"type":"http","uris":["https://static.imply.io/data/wikipedia.json.gz"]}',
       '{"type":"json"}',
       '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
     )
   ))
   SELECT
     TIME_PARSE("timestamp") AS __time,
     "isRobot",
     "channel",
     "flags",
     "isUnpatrolled",
     "page",
     "diffUrl",
     "added",
     "comment",
     "commentLength",
     "isNew",
     "isMinor",
     "delta",
     "isAnonymous",
     "user",
     "deltaBucket",
     "deleted",
     "namespace",
     "cityName",
     "countryName",
     "regionIsoCode",
     "metroCode",
     "countryIsoCode",
     "regionName"
   FROM ioConfigExtern
   PARTITIONED BY DAY   
   ```
   
   </details>

4. Review the generated SQL to make sure it matches your requirements and does what you expect.
5. Click **Run** to start the ingestion.


## Next steps

To prepare to start ingesting your data, do the following:

- Review the [security details](./msqe-security.md) for MSQE, so that you understand what kind of access is required for your datasources in order to use MSQE. 
- Read more about how [MSQE query syntax](./msqe-sql-syntax.md)
- Look at the example queries to see examples of what you can do with MSQE.

## Example queries

These example queries show you some of the other things you can do to modify your MSQE queries depending on your use case. You can copy the example queries into the **Query** UI and run them.

### INSERT with no rollup

This example inserts data into a table named `w000` without performing any data rollup:

<details><summary>Show the query</summary>

```sql
--:context msqFinalizeAggregations: false
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

</details>

### INSERT with rollup

This example inserts data into a table named `kttm_data` and performs data rollup. This example implements the recommendations described in [multi-value dimensions](./msqe-sql-syntax.md#multi-value-dimensions).

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