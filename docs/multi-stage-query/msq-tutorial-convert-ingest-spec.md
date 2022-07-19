---
id: convert-json-spec
title: Tutorial - Convert ingestion spec
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

Before you start, make sure you've [enabled the Multi-Stage Query (MSQ) framework](msq-setup.md).

If you're already ingesting data with Druid's core query engine, you can use the Druid console to help you convert the ingestion spec to a task query that MSQ can use to ingest data.

In the **Query** view, do the following:

1. In the menu bar that includes **Run**, select **...** **>** **Convert ingestion spec to SQL**.
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

   This is what the query looks like for the sample ingestion spec:

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