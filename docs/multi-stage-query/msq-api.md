---
id: api
title: API
---

> The Multi-Stage Query (MSQ) framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

> Earlier versions of MSQ  used the `/druid/v2/sql/async/` end point. The engine now uses different endpoints in version 2022.05 and later. Some actions use the `/druid/v2/sql/task` while others use the `/druid/indexer/v1/task/` endpoint . Additionally, you no longer need to set a context parameter for `talaria`. API calls to the `task` endpoint use the task engine for MSQ automatically.

During the preview phase, the enhanced Query view provides the most stable experience. Use the UI if you do not need a programmatic interface.

The action you want to take determines the endpoint you use:

- `/druid/v2/sql/task` endpoint: Submit a query for ingestion.
- `/druid/indexer/v1/task` endpoint: Interact with a query, including getting its status, getting its details, or canceling it.

## Submit a task query

### Request

Submit task queries using the `POST /druid/v2/sql/task/` API.

Currently, MSQE ignores the provided values of `resultFormat`, `header`,
`typesHeader`, and `sqlTypesHeader`. SQL SELECT queries always behave if `resultFormat` is "array", `header` is
true, `typesHeader` is true, and `sqlTypesHeader` is true.

For task queries like the examples in the [quickstart](./msq-queries.md#query-examples), you need to escape characters such as quotation marks (") if you use something like `curl`. If you use a method that can parse JSON seamlessly like Python, you don't need to. The following example does though.

The following example is the same query that you submit when you complete [Convert a JSON ingestion spec](./msq-tutorial-convert-ingest-spec.md) where you insert data into a table named `wikipedia`. Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.

<!--DOCUSAURUS_CODE_TABS-->

<!--HTTP-->

```
POST /druid/v2/sql/task
```

```json
{
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
      "msqMaxNumTasks": 3
  }
}
```

<!--curl-->

```bash
# Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.
curl --location --request POST 'https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '\''{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}'\'',\n    '\''{\"type\": \"json\"}'\'',\n    '\''[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\''\n  )\n)\nPARTITIONED BY DAY",
    "context": {
        "msqMaxNumTasks": 3
    }
```

<!--Python-->

```python
# Make sure you replace `username`, `password`, `your-instance`, and `port` with the values for your deployment.
import json
import requests

url = "https://<username>:<password>@<your-instance>:<port>/druid/v2/sql/task/"

payload = json.dumps({
  "query": "INSERT INTO wikipedia\nSELECT\n  TIME_PARSE(\"timestamp\") AS __time,\n  *\nFROM TABLE(\n  EXTERN(\n    '{\"type\": \"http\", \"uris\": [\"https://static.imply.io/gianm/wikipedia-2016-06-27-sampled.json\"]}',\n    '{\"type\": \"json\"}',\n    '[{\"name\": \"added\", \"type\": \"long\"}, {\"name\": \"channel\", \"type\": \"string\"}, {\"name\": \"cityName\", \"type\": \"string\"}, {\"name\": \"comment\", \"type\": \"string\"}, {\"name\": \"commentLength\", \"type\": \"long\"}, {\"name\": \"countryIsoCode\", \"type\": \"string\"}, {\"name\": \"countryName\", \"type\": \"string\"}, {\"name\": \"deleted\", \"type\": \"long\"}, {\"name\": \"delta\", \"type\": \"long\"}, {\"name\": \"deltaBucket\", \"type\": \"string\"}, {\"name\": \"diffUrl\", \"type\": \"string\"}, {\"name\": \"flags\", \"type\": \"string\"}, {\"name\": \"isAnonymous\", \"type\": \"string\"}, {\"name\": \"isMinor\", \"type\": \"string\"}, {\"name\": \"isNew\", \"type\": \"string\"}, {\"name\": \"isRobot\", \"type\": \"string\"}, {\"name\": \"isUnpatrolled\", \"type\": \"string\"}, {\"name\": \"metroCode\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"regionIsoCode\", \"type\": \"string\"}, {\"name\": \"regionName\", \"type\": \"string\"}, {\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": {
    "msqMaxNumTasks": 3
  }
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

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
|taskId|Controller task ID.<br /><br />Druid's standard [task APIs](../operations/api-reference.md#overlord) can be used to interact with this controller task.|
|state|Initial state for the query, which is "RUNNING".|


## Get the payload for a task query

- `GET /druid/indexer/v1/task/{taskId}` to get the query payload

## Get the status for a task query

- `GET /druid/indexer/v1/task/{taskId}/status` to get the query status

## Get the report for a task query

Keep the following in mind when using the task API to view reports:

- For SELECT queries, the results are included in the report. At this time, if you want to view results for SELECT queries, you need to retrieve them as a generic map from the report and extract the results.
- Query details are physically stored in the task report for controller tasks.
- If you encounter 500 Server Error or 404 Not Found errors, the task may be in the process of starting up or shutting down.

For information about the report fields, see [Report response fields](#report-response-fields).

- `GET /druid/indexer/v1/task/{taskId}/reports` to get the query report

### Report response fields

The following table describes the response fields when you retrieve a report for a MSQE task using the `/druid/indexer/v1/task` endpoint:

|Field|Description|
|-----|-----------|
|multiStageQuery.taskId|Controller task ID.|
|multiStageQuery.payload.status|Query status container.|
|multiStageQuery.payload.status.status|RUNNING, SUCCESS, or FAILED.|
|multiStageQuery.payload.status.startTime|Start time of the query in ISO format. Only present if the query has started running.|
|multiStageQuery.payload.status.durationMs|Milliseconds elapsed after the query has started running. -1 denotes that the query hasn't started running yet.|
|multiStageQuery.payload.status.errorReport|Error object. Only present if there was an error.|
|multiStageQuery.payload.status.errorReport.taskId|The task that reported the error, if known. May be a controller task or a worker task.|
|multiStageQuery.payload.status.errorReport.host|The hostname and port of the task that reported the error, if known.|
|multiStageQuery.payload.status.errorReport.stageNumber|The stage number that reported the error, if it happened during execution of a specific stage.|
|multiStageQuery.payload.status.errorReport.error|Error object. Contains `errorCode` at a minimum, and may contain other fields as described in the [error code table](./msq-concepts.md#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorCode|One of the error codes from the [error code table](./msq-concepts.md#error-codes). Always present if there is an error.|
|multiStageQuery.payload.status.errorReport.error.errorMessage|User-friendly error message. Not always present, even if there is an error.|
|multiStageQuery.payload.status.errorReport.exceptionStackTrace|Java stack trace in string form, if the error was due to a server-side exception.|
|multiStageQuery.payload.stages|Array of query stages.|
|multiStageQuery.payload.stages[].stageNumber|Each stage has a number that differentiates it from other stages.|
|multiStageQuery.payload.stages[].inputStages|Array of input stage numbers.|
|multiStageQuery.payload.stages[].stageType|String that describes the logic of this stage. This is not necessarily unique across stages.|
|multiStageQuery.payload.stages[].phase|Either NEW, READING_INPUT, POST_READING, RESULTS_COMPLETE, or FAILED. Only present if the stage has started.|
|multiStageQuery.payload.stages[].workerCount|Number of parallel tasks that this stage is running on. Only present if the stage has started.|
|multiStageQuery.payload.stages[].partitionCount|Number of output partitions generated by this stage. Only present if the stage has started and has computed its number of output partitions.|
|multiStageQuery.payload.stages[].inputFileCount|Number of external input files or Druid segments read by this stage. Does not include inputs from other stages in the same query.|
|multiStageQuery.payload.stages[].startTime|Start time of this stage. Only present if the stage has started.|
|multiStageQuery.payload.stages[].query|Native Druid query for this stage. Only present for the first stage that corresponds to a particular native Druid query.|

## Cancel a task query

- `POST /druid/indexer/v1/task/{taskId}/shutdown` to cancel a query

