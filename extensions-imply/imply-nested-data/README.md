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
      },
      "transforms": [
        {
          "type": "expression",
          "name": "msg_x",
          "expression": "json_value(msg, '$.x')"
        },
        {
          "type": "expression",
          "name": "msg_identity",
          "expression": "json_query(msg, '$.')"
        },
        {
          "type": "expression",
          "name": "msg_reconstruct",
          "expression": "json_object('x', json_value(msg, '$.x'), 'y', json_value(x, '$.y'))"
        }
      ]
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
| `JSON_VALUE(expr, path)` | Extract a Druid literal (`STRING`, `LONG`, `DOUBLE`) value from a `COMPLEX<json>` column or input `expr` using JSONPath syntax of `path` |
| `JSON_QUERY(expr, path)` | Extract a `COMPLEX<json>` value from a `COMPLEX<json>` column or input `expr` using JSONPath syntax of `path` |
| `JSON_OBJECT(KEY expr1 VALUE expr2[, KEY expr3 VALUE expr4 ...])` | Construct a `COMPLEX<json>` storing the results of `VALUE` expressions at `KEY` expressions |
| `PARSE_JSON(expr)` | Deserialize a JSON `STRING` into a `COMPLEX<json>` to be used with expressions which operate on `COMPLEX<json>` inputs. |
| `TO_JSON(expr)` | Convert any input type to `COMPLEX<json>` to be used with expressions which operate on `COMPLEX<json>` inputs, like a `CAST` operation (rather than deserializing `STRING` values like `PARSE_JSON`) |
| `TO_JSON_STRING(expr)` | Convert a `COMPLEX<json>` input into a JSON `STRING` value |
| `JSON_KEYS(expr, path)`| get array of field names in `expr` at the specified JSONPath `path`, or null if the data does not exist or have any fields |
| `JSON_PATHS(expr)` | get array of all JSONPath paths available in `expr` |