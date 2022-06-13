---
id: msqe-api
title: API
---

> The multi-stage query engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

> Earlier versions of the multi-stage query engine used the `/druid/v2/sql/async/` end point. The engine now uses different endpoints in version 2022.05 and later. Some actions use the `/druid/v2/sql/task` while others use the `/druid/indexer/v1/task/` endpoint . Additionally, you no longer need to set a context parameter for `talaria`. API calls to the `task` endpoint use the multi-stage query engine automatically.

During the preview phase, the enhanced Query view will provide the most stable experience. Use the UI if you do not need a programmatic interface.

Queries for the Multi-Stage Query Engine (MSQE) run as tasks. The action you want to take determines the endpoint you use:

- `/druid/v2/sql/task` endpoint: Submit a query for ingestion.
- `/druid/indexer/v1/task` endpoint: Interact with a query, including getting its status, getting its details, or canceling it. 

## Submit a query

### Request

Submit queries using the `POST /druid/v2/sql/task/` API.

Currently, MSQE ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. SQL SELECT queries always behave if `resultFormat` is "array", `header` is
true, `typesHeader` is true, and `sqlTypesHeader` is true.

For queries like the examples in the [quickstart](./msqe-quickstart.md), you need to escape characters such as quotation marks (") if you use something like `curl`. If you use a method that can parse JSON seamlessly like Python, you don't need to. The following example does though.

The following example is the same query that you submit when you complete [Convert a JSON ingestion spec](./msqe-quickstart.md#convert-a-json-ingestion-spec) where you insert data into a table named `wikipedia`. Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.

<!--DOCUSAURUS_CODE_TABS-->

<!--cURL-->

```bash
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "multiStageQuery": true
    }
```

<!--Python-->

```python
import json
import requests

url = "https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "multiStageQuery": True
  }
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

```

<!--HTTP-->

```
POST /druid/v2/sql/task
```

```json
{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "multiStageQuery": true
    }
}
```
<!--END_DOCUSAURUS_CODE_TABS-->


### Response

```json
{
  "taskId": "query-f795a235-4dc7-4fef-abac-3ae3f9686b79",
  "state": "RUNNING",
}
```

**Response fields**

|Field|Description|
|-----|-----------|
|taskId|Controller task ID.<br /><br />Druid's standard [task APIs](https://docs.imply.io/latest/druid/operations/api-reference.html#overlord) can be used to interact with this controller task.|
|state|Initial state for the query, which is "RUNNING".|

## Interact with a query

Because queries run as Overlord tasks, use the [task APIs](/operations/api-reference#overlord) to interact with a query.

When using MSQE, the endpoints you frequently use may include:

- `GET /druid/indexer/v1/task/{taskId}` to get the query payload
- `GET /druid/indexer/v1/task/{taskId}/status` to get the query status
- `GET /druid/indexer/v1/task/{taskId}/reports` to get the query report
- `POST /druid/indexer/v1/task/{taskId}/shutdown` to cancel a query

For SELECT queries, you need write access for the source datasource. For INSERT and REPLACE queries, you need write access for the target datasource.

### MSQE reports

Keep the following in mind when using the task API to view reports:

- For SELECT queries, the results are included in the report. At this time, if you want to view results for SELECT queries, you need to retrieve them as a generic map from the report and extract the results.
- Query details are physically stored in the task report for controller tasks.
- If you encounter 500 Server Error or 404 Not Found errors, the task may be in the process of starting up or shutting down.



