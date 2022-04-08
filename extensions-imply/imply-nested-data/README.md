<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->


# Druid nested 'JSON' columns
This extension adds support for storing structured/nested data in Druid, without employing flattenSpec. 

To use, be sure to add `imply-nested-data` to the extension load list.


## Ingesting data

To store and index JSON columns, specify the `json` type in the dimension spec of a native ingestion job:

```
{
  "type": "index_parallel",
  "spec": {
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": [
          "https://static.imply.io/vadim/scrap/nested_test1.json"
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
      "dataSource": "nested_test1",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          "host",
          "service",
          {
            "type": "json",
            "name": "msg"
          }
        ]
      },
      "granularitySpec": {
        "queryGranularity": "none",
        "rollup": false,
        "segmentGranularity": "year"
      }
    }
  }
}
```

and data will be ingested and stored as `COMPLEX<json>` typed. For Talaria queries, specify this as the column type when defining the row signature:

```
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/vadim/scrap/nested_test1.json"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"host","type":"string"},{"name":"service","type":"string"},{"name":"msg","type":"COMPLEX<json>"}]'
  )
)
```

## Querying data

| function | notes |
|---|---|
| `JSON_GET_PATH(expr, path)` | get a literal value from a nested JSON column `expr` using "jq" syntax of `path` |
| `JSON_KEYS(expr, path)`| get array of field names in `expr` at the specified "jq" `path`, or null if the data does not exist or have any fields |
| `JSON_PATHS(expr)` | get array of all `jq` paths available in `expr` |
