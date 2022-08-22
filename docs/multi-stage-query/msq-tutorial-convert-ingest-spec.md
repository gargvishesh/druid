---
id: convert-json-spec
title: Tutorial - Convert ingestion spec to SQL
---

> The multi-stage query architecture and its SQL-task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

If you're already ingesting data with Druid's native SQL engine, you can use the Druid console to convert the ingestion spec to a task query that the multi-stage architecture's SQL-task engine can use to ingest data.

This tutorial demonstrates how to convert the ingestion spec to a task query in the Druid console.

## Convert ingestion spec to task query

To convert the ingestion spec to a task query, do the following:

1. In the **Query** view of the Druid console, navigate to the menu bar that includes **Run**.
2. Click the ellipsis icon and select **Convert ingestion spec to SQL**.
  ![Convert ingestion spec to SQL](../assets/multi-stage-query/tutorial-msq-convert.png "Convert ingestion spec to SQL")
3. In the **Ingestion spec to covert** window, insert your ingestion spec. You can use your own spec or the sample ingestion spec provided in the tutorial. The sample spec uses data hosted at `https://static.imply.io/data/wikipedia.json.gz` and loads it into a table named `wikipedia`:

   <details><summary>Show the spec</summary>
   
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

4. Click **Submit** to submit the spec. The Druid console uses the JSON-based ingestion spec to generate a SQL query that you can use instead. This is what the query looks like for the sample ingestion spec:
   
   <details><summary>Show the query</summary>

   ```sql
   -- This SQL query was auto generated from an ingestion spec
   REPLACE INTO wikipedia OVERWRITE ALL
   WITH source AS (SELECT * FROM TABLE(
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
   FROM source
   PARTITIONED BY DAY 
   ```
   
   </details>

4. Review the generated SQL query to make sure it matches your requirements and does what you expect.
5. Click **Run** to start the ingestion.