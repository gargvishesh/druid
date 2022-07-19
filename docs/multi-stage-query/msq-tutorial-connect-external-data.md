---
id: connect-external-data
title: Tutorial - Connect external data
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

Before you start, make sure you've [enabled the Multi-Stage Query (MSQ) Framework](./msq-setup.md).

The following section takes you through the **Connect external data** wizard that helps you create a query that references externally hosted data. 

## Examine and load external data

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

   - The task query includes context parameters.  The syntax is unique to the Console: `--: context {key}: {value}`. When submitting queries to Druid directly, set the context parameters in the context section of the SQL query object. For more information about context parameters, see [Context parameters](./msq-reference.md#context-parameters).
   - For information about what the different parts of this query are, see [Queries](./msq-queries.md).
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

7. Optionally, select **Preview** to review the data before you ingest it. A preview runs the query without the INSERT into clause and with an added LIMIT to the main query and to all helper queries. You can see the general shape of the data before you commit to inserting it. The  LIMITs make the query run faster but can cause incomplete results.
8. Run your query. The query returns information including the number of rows inserted into the table named `wikipedia-2016-06-27-sampled` and how long the query took.

## Query the data

The data that you loaded into `wikipedia-2016-06-27-sampled` can be queried after the ingestion completes. You can analyze the data in the table to do things like produce a list of top channels:

```sql
SELECT
  channel,
  COUNT(*)
FROM "wikipedia-2016-06-27-sampled"
GROUP BY channel
ORDER BY COUNT(*) DESC
```

With the EXTERN function, you could run the same query on the external data directly without ingesting it first:

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

