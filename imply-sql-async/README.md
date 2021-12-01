<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Asynchronous SQL query downloads

Asynchronous query downloads (async downloads) lets you run longer-executing queries and retrieve the results.  It solves problems caused when timeouts cause interruptions in the connection between query clients and the Druid cluster.
The user experience with async download APIs is similar to using the synchronous APIs. Instead of one synchronous API, async download requires you to call three APIs to:
- Submit the query.
- Poll for query status.
- Fetch the result.

Async downloads does not:
- Provide file management APIs. Druid is not a file management system, therefore Druid does not expose the concept of the file containing the results to users.
- Support long retention periods for the query results. You should write your query to fetch the query result as soon as possible. The client cannot use deep storage as a query cache layer.

The current state of this feature is _alpha_. The APIs, properties, metrics explained in this doc can change any time.

## Setup

To enable async query downloads, add the following properties in `common.runtime.properties`.


### Enabling async downloads

|config|description|required|default|
| --- | --- | --- | --- |
|`druid.query.async.sql.enabled`| Must be set to `true` to use async downloads. | yes | `false` |


### Query result storage

Async downloads support:
- local file storage on the broker, the default. The local file storage is recommended to use only for testing.
- Amazon S3 storage.


#### Local storage

The local storage uses a local disk on the broker to store result files.
This storage is recommended to use only for testing.

|config|description|required|default|
| --- | --- | --- | --- |
|`druid.query.async.storage.type`| Must be set to `local`. | no | `local` |
|`druid.query.async.storage.local.directory`| Directory to store query results. | yes when local storage is used | |

The following example shows the configuration for local storage:

```
druid.query.async.storage.type=local
druid.query.async.storage.local.directory=/path/to/your/directory
```

#### S3

The `druid-s3-extensions` must be loaded to use s3 result storage.

|config|description|required|default|
| --- | --- | --- | --- |
|`druid.query.async.storage.type`| Must be set to `s3`. | yes | `local` |
|`druid.query.async.storage.s3.bucket`| S3 bucket to store query results. | yes | |
|`druid.query.async.storage.s3.prefix`| S3 prefix to store query results. Do not share the same path with other objects. The auto cleanup coordinatory duty will delete all objects under the same path if it thinks they are expired. | yes | |
|`druid.query.async.storage.s3.tempDir`| Directory path in local disk to store query results temporarily. | yes | |
|`druid.query.async.storage.s3.maxResultsSize`| Max size of each result file. It should be between 5MiB and 5TiB. Supports human-readable format. | no | 100MiB |
|`druid.query.async.storage.s3.maxTotalResultsSize`| Max total size of all query result files. Supports human-readable format. | no | 5GiB |
|`druid.query.async.storage.s3.chunkSize`| This property is intended only to support rare cases. This property defines the size of each chunk to temporarily store in `druid.query.async.storage.s3.tempDir`.  Druid computes the chunk size automatically when this property is not set. The chunk size must be between 5MiB and 5GiB. | no | null |
|`druid.query.async.storage.s3.maxTriesOnTransientErrors`| This property is intended only to support rare cases. This property defines the max number times to attempt s3 API calls to avoid failures due to transient errors. | no | 10 |

The following example shows the configuration for S3 storage:

```
druid.query.async.storage.type=s3
druid.query.async.storage.s3.bucket=your_bucket
druid.query.async.storage.s3.prefix=your_prefix
druid.query.async.storage.s3.tempDir=/path/to/your/temp/dir
druid.query.async.storage.s3.maxResultsSize=1GiB
druid.query.async.storage.s3.maxTotalResultsSize=10GiB
```


##### S3 permissions/lifecycle settings

`s3:GetObject`, `s3:PutObject`, and `s3:AbortMultipartUpload` are required for pushing/fetching query results to/from S3.
`s3:DeleteObject` and `s3:ListBucket` are required for cleaning up expired results.

When result file upload fails, Druid will abort the upload to clean up partially uploaded files. However, if the abort
fails after a couple of retries, those partially uploaded files can remain in your s3 bucket. To handle them, it is recommended to set
a lifecycle rule using `AbortIncompleteMultipartUpload` for your s3 bucket and prefix with `DaysAfterInitiation`.
A recommended value for `DaysAfterInitiation` is 2 * query timeout.

### Query state and result file management

Async downloads uses two coordinator duties to clean up expired query states and result files: `killAsyncQueryMetadata`, `killAsyncQueryResultWithoutMetadata`, and `updateStaleQueryState`.
When you turn on async downloads, Druid enables these duties automatically.
These duties support the following properties:

|config|description|required|default|
| --- | --- | --- | --- |
|`druid.query.async.cleanup.timeToRetain`| Retention period of query states and result files. Supports the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. | no | PT60S |
|`druid.query.async.cleanup.timeToWaitAfterBrokerGone`| Duration to wait after a missing broker is detected by coordinator. If a broker has gone offline for longer than this duration, the coordiantor will mark query states `UNDETERMINED` for the queries that were running in the offline broker. Supports the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) duration format. | no | PT1M |
|`druid.query.async.cleanup.pollPeriod`| Duty group run period. Must be a form of `PT{n}S` to set this to `n` seconds. | no | PT30S |

#### Configuring coordinator duties

The coordinator duties for cleaning up query states and results are executed periodically per `druid.query.async.cleanup.pollPeriod`.
As a result, any time-based configuration for duties will be affected by the `pollPeriod`. For example, if you set
`druid.query.async.cleanup.timeToWaitAfterBrokerGone` and `druid.query.async.cleanup.pollPeriod` to `PT40S` and `PT30S`, respectively,
the actual effective `timeToWaitAfterBrokerGone` will be `PT60S` because the `updateStaleQueryState` duty will check
whether the broker has gone offline for longer than `timeToWaitAfterBrokerGone` every 30 seconds.

### Query execution limits

|config|description|required|default|
| --- | --- | --- | --- |
|`druid.query.async.maxConcurrentQueries`| Max number of active queries that can run concurrently in each Broker. The Broker queues any queries that exceed the active limit. | no | 10% of number of physical cores in the broker |
|`druid.query.async.maxQueriesToQueue`| Max number of queries to store in the Broker queue. The Broker rejects any queries that exceed the limit. | no | `max(10, maxConcurrentQueries * 3)` |


## APIs

### Submitting a query

#### Request

```
POST /druid/v2/sql/async/
```

To start a query, POST a request to the /druid/v2/sql/async/ endpoint in the same format as [SQL sync API](https://druid.apache.org/docs/latest/querying/sql.html#http-post).

#### Response

When the API succeeds, it returns a JSON object with the following fields:

- `asyncResultId`: Unique ID for the query. This ID can be different from the queryId for native or sql queries. Always present.
- `state`: Query state. Always present. Possible states:
  - `INITIALIZED`: The query has been set up but has not yet started running.
  - `RUNNING`: The query has started running. The time it takes for queries to move from `INITIALIZED` to `RUNNING` depends on the load state of the system.
  - `COMPLETE`: The query has finished and results are ready to fetch.
  - `FAILED`: The query has failed.
  - `UNDETERMINED`: The query state is unknown. This state can be returned if Druid is aware of the query, but cannot determine the valid query state. This is different from the unknown queries.
- `resultFormat`: Result format for this query, taken from the list at [SQL sync API](https://druid.apache.org/docs/latest/querying/sql.html#http-post). Present when state is `COMPLETE`, absent otherwise.
- `resultLength`: Size in bytes of the query results. Present when state is `COMPLETE`, absent otherwise.
- `error`: Druid query error object, as described at [Query execution failures](https://druid.apache.org/docs/latest/querying/querying.html#query-execution-failures) . Present when state is `FAILED`, absent otherwise.

HTTP 202 if the query has been accepted. The returned query status object will be in state "INITIALIZED". You can use the Query Status API to check on its status.

HTTP 429 if the query has been rejected due to concurrency limits. Callers should retry the query after waiting an appropriate amount of time. Exponential backoff is encouraged. The returned query status object will be in state "FAILED". The Query Status API will return 404 for this async result ID.

HTTP 4xx (not 429) or 5xx if the query has been rejected for some other reason. The returned query status object will be in state "FAILED". The Query Status API will return 404 for this async result ID.


### Getting query status

After a query has been started, its status can be checked using the below API.

#### Request

```
GET /druid/v2/sql/async/{asyncResultId}/status
```

#### Response

When the API succeeds, it returns the same JSON object as described in the previous section.

HTTP 404 if the async result ID does not exist or if the async result ID has expired.

HTTP 403 if the user is not authorized to get the query status.

### Getting query results

After a query has been completed, you can fetch the results. Attempting to fetch results before query completion returns
a 404. This API should return the same results for the same query during the async download retention period, no matter
how many times you call the API.

#### Request

```
GET /druid/v2/sql/async/{asyncResultId}/results
```

#### Response

The file, if it is ready.

HTTP 404 if the async result ID does not exist, if the async result ID has expired, if the async result ID is not in
state `COMPLETE`.

HTTP 403 if the user is not authorized to get the query results.

The following headers will be set on a successful response:

- Content-Length is set to the result file size.
- Content-Type is set based on the query resultFormat as described
  in [SQL response](https://druid.apache.org/docs/latest/querying/sql.html#responses).
- Content-Disposition is set to "attachment"

### Cancelling a query

After a query has been submitted, you can cancel the query. Canceling a query cleans up all records of it, as if it
never happened. Queries can be canceled while in any state.

#### Request

```
DELETE /druid/v2/sql/async/{asyncResultId}
```

#### Response

HTTP 404 if the query ID does not exist, if the query ID has expired, if the query ID originated by a different user, or
if the Broker running the query goes offline.

HTTP 403 if the user is not authorized to cancel.

HTTP 202 if the deletion request has been accepted.

HTTP 500 in case of internal server error eg:

```
{
  "asyncResultId":"xxxx",
  "error":"unable to clear metadata",
}
```

The actual cancellation will proceed in the background. It is possible that immediately after cancellation, the status
API will still return details for the query.

## Metrics

### Broker metrics

- `async/result/tracked/count`: number of queries tracked by Druid.
- `async/result/tracked/bytes`: Total results query size tracked by Druid.
- `async/sqlQuery/running/count`: number of running queries.
- `async/sqlQuery/queued/count`: number of queued queries.
- `async/sqlQuery/running/max`: max number of running queries. Must be the same as `druid.query.async.maxConcurrentQueries`.
- `async/sqlQuery/queued/max`: max number of queued queries. Must be the same as `druid.query.async.maxQueriesToQueue`.

### Coordinator metrics

- `async/cleanup/result/removed/count`: number of query result files successfully deleted in each coordinator run.
- `async/cleanup/result/failed/count`: number of failed attempts to delete query results in each coordinator run.
- `async/cleanup/metadata/removed/count`: number of query states successfully cleaned up in each coordinator run.
- `async/cleanup/metadata/failed/count`: number of failed attempts to clean up query states in each coordinator run.
- `async/cleanup/metadata/skipped/count`: number of query states that have not expired in each coordinator run.
- `async/query/undetermined/count`: number of queries that have been marked as `UNDETERMINED` during each `druid.query.async.cleanup.pollPeriod`.


## Known issues

- When the broker shuts down gracefully, it marks all queries running in it as `FAILED` before it completely shuts down.
  However, if the broker fails before it finishes updating the query status, the states of queries in progress remain unchanged.
  In this case, you must manually clean up the queries left in invalid states.
- Even when the broker shuts down gracefully, it currently doesn't wait for running queries in it to complete before
  it completely shuts down. Instead, it simply marks them `FAILED`. This can fail your queries during rolling updates.