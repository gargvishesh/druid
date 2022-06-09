/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Execution } from './execution';

describe('Execution', () => {
  describe('.fromAsyncStatus', () => {
    it('works', () => {
      expect(
        Execution.fromAsyncStatus({
          error: {
            error: 'SQL parse failed',
            errorMessage:
              'Encountered "ALL LIMIT" at line 10, column 16.\nWas expecting one of:\n    "HOUR" ...\n    "DAY" ...\n    "MONTH" ...\n    "YEAR" ...\n    "ALL" "TIME" ...\n    "+" ...\n    "-" ...\n    "NOT" ...\n    "EXISTS" ...\n    <UNSIGNED_INTEGER_LITERAL> ...\n    <DECIMAL_NUMERIC_LITERAL> ...\n    <APPROX_NUMERIC_LITERAL> ...\n    <BINARY_STRING_LITERAL> ...\n    <PREFIXED_STRING_LITERAL> ...\n    <QUOTED_STRING> ...\n    <UNICODE_STRING_LITERAL> ...\n    "TRUE" ...\n    "FALSE" ...\n    "UNKNOWN" ...\n    "NULL" ...\n    <LBRACE_D> ...\n    <LBRACE_T> ...\n    <LBRACE_TS> ...\n    "DATE" ...\n    "TIME" ...\n    "TIMESTAMP" ...\n    "INTERVAL" ...\n    "?" ...\n    "CAST" ...\n    "EXTRACT" ...\n    "POSITION" ...\n    "CONVERT" ...\n    "TRANSLATE" ...\n    "OVERLAY" ...\n    "FLOOR" ...\n    "CEIL" ...\n    "CEILING" ...\n    "SUBSTRING" ...\n    "TRIM" ...\n    "CLASSIFIER" ...\n    "MATCH_NUMBER" ...\n    "RUNNING" ...\n    "PREV" ...\n    "NEXT" ...\n    "JSON_EXISTS" ...\n    "JSON_VALUE" ...\n    "JSON_QUERY" ...\n    "JSON_OBJECT" ...\n    "JSON_OBJECTAGG" ...\n    "JSON_ARRAY" ...\n    "JSON_ARRAYAGG" ...\n    <LBRACE_FN> ...\n    "MULTISET" ...\n    "ARRAY" ...\n    "PERIOD" ...\n    "SPECIFIC" ...\n    <IDENTIFIER> ...\n    <QUOTED_IDENTIFIER> ...\n    <BACK_QUOTED_IDENTIFIER> ...\n    <BRACKET_QUOTED_IDENTIFIER> ...\n    <UNICODE_QUOTED_IDENTIFIER> ...\n    "ABS" ...\n    "AVG" ...\n    "CARDINALITY" ...\n    "CHAR_LENGTH" ...\n    "CHARACTER_LENGTH" ...\n    "COALESCE" ...\n    "COLLECT" ...\n    "COVAR_POP" ...\n    "COVAR_SAMP" ...\n    "CUME_DIST" ...\n    "COUNT" ...\n    "CURRENT_DATE" ...\n    "CURRENT_TIME" ...\n    "CURRENT_TIMESTAMP" ...\n    "DENSE_RANK" ...\n    "ELEMENT" ...\n    "EXP" ...\n    "FIRST_VALUE" ...\n    "FUSION" ...\n    "GROUPING" ...\n    "LAG" ...\n    "LEAD" ...\n    "LEFT" ...\n    "LAST_VALUE" ...\n    "LN" ...\n    "LOCALTIME" ...\n    "LOCALTIMESTAMP" ...\n    "LOWER" ...\n    "MAX" ...\n    "MIN" ...\n    "MINUTE" ...\n    "MOD" ...\n    "NTH_VALUE" ...\n    "NTILE" ...\n    "NULLIF" ...\n    "OCTET_LENGTH" ...\n    "PERCENT_RANK" ...\n    "POWER" ...\n    "RANK" ...\n    "REGR_COUNT" ...\n    "REGR_SXX" ...\n    "REGR_SYY" ...\n    "RIGHT" ...\n    "ROW_NUMBER" ...\n    "SECOND" ...\n    "SQRT" ...\n    "STDDEV_POP" ...\n    "STDDEV_SAMP" ...\n    "SUM" ...\n    "UPPER" ...\n    "TRUNCATE" ...\n    "USER" ...\n    "VAR_POP" ...\n    "VAR_SAMP" ...\n    "CURRENT_CATALOG" ...\n    "CURRENT_DEFAULT_TRANSFORM_GROUP" ...\n    "CURRENT_PATH" ...\n    "CURRENT_ROLE" ...\n    "CURRENT_SCHEMA" ...\n    "CURRENT_USER" ...\n    "SESSION_USER" ...\n    "SYSTEM_USER" ...\n    "NEW" ...\n    "CASE" ...\n    "CURRENT" ...\n    "CURSOR" ...\n    "ROW" ...\n    "(" ...\n    ',
            errorClass: 'org.apache.calcite.sql.parser.SqlParseException',
            host: null,
          },

          asyncResultId: '8c50267b-ca15-4001-8b69-3d3b5c0db932',
          state: 'UNDETERMINED',
        }),
      ).toMatchInlineSnapshot(`
        Execution {
          "_payload": undefined,
          "destination": undefined,
          "duration": undefined,
          "engine": "sql-async",
          "error": Object {
            "error": Object {
              "errorCode": "AsyncError",
              "errorMessage": "{\\"error\\":\\"SQL parse failed\\",\\"errorMessage\\":\\"Encountered \\\\\\"ALL LIMIT\\\\\\" at line 10, column 16.\\\\nWas expecting one of:\\\\n    \\\\\\"HOUR\\\\\\" ...\\\\n    \\\\\\"DAY\\\\\\" ...\\\\n    \\\\\\"MONTH\\\\\\" ...\\\\n    \\\\\\"YEAR\\\\\\" ...\\\\n    \\\\\\"ALL\\\\\\" \\\\\\"TIME\\\\\\" ...\\\\n    \\\\\\"+\\\\\\" ...\\\\n    \\\\\\"-\\\\\\" ...\\\\n    \\\\\\"NOT\\\\\\" ...\\\\n    \\\\\\"EXISTS\\\\\\" ...\\\\n    <UNSIGNED_INTEGER_LITERAL> ...\\\\n    <DECIMAL_NUMERIC_LITERAL> ...\\\\n    <APPROX_NUMERIC_LITERAL> ...\\\\n    <BINARY_STRING_LITERAL> ...\\\\n    <PREFIXED_STRING_LITERAL> ...\\\\n    <QUOTED_STRING> ...\\\\n    <UNICODE_STRING_LITERAL> ...\\\\n    \\\\\\"TRUE\\\\\\" ...\\\\n    \\\\\\"FALSE\\\\\\" ...\\\\n    \\\\\\"UNKNOWN\\\\\\" ...\\\\n    \\\\\\"NULL\\\\\\" ...\\\\n    <LBRACE_D> ...\\\\n    <LBRACE_T> ...\\\\n    <LBRACE_TS> ...\\\\n    \\\\\\"DATE\\\\\\" ...\\\\n    \\\\\\"TIME\\\\\\" ...\\\\n    \\\\\\"TIMESTAMP\\\\\\" ...\\\\n    \\\\\\"INTERVAL\\\\\\" ...\\\\n    \\\\\\"?\\\\\\" ...\\\\n    \\\\\\"CAST\\\\\\" ...\\\\n    \\\\\\"EXTRACT\\\\\\" ...\\\\n    \\\\\\"POSITION\\\\\\" ...\\\\n    \\\\\\"CONVERT\\\\\\" ...\\\\n    \\\\\\"TRANSLATE\\\\\\" ...\\\\n    \\\\\\"OVERLAY\\\\\\" ...\\\\n    \\\\\\"FLOOR\\\\\\" ...\\\\n    \\\\\\"CEIL\\\\\\" ...\\\\n    \\\\\\"CEILING\\\\\\" ...\\\\n    \\\\\\"SUBSTRING\\\\\\" ...\\\\n    \\\\\\"TRIM\\\\\\" ...\\\\n    \\\\\\"CLASSIFIER\\\\\\" ...\\\\n    \\\\\\"MATCH_NUMBER\\\\\\" ...\\\\n    \\\\\\"RUNNING\\\\\\" ...\\\\n    \\\\\\"PREV\\\\\\" ...\\\\n    \\\\\\"NEXT\\\\\\" ...\\\\n    \\\\\\"JSON_EXISTS\\\\\\" ...\\\\n    \\\\\\"JSON_VALUE\\\\\\" ...\\\\n    \\\\\\"JSON_QUERY\\\\\\" ...\\\\n    \\\\\\"JSON_OBJECT\\\\\\" ...\\\\n    \\\\\\"JSON_OBJECTAGG\\\\\\" ...\\\\n    \\\\\\"JSON_ARRAY\\\\\\" ...\\\\n    \\\\\\"JSON_ARRAYAGG\\\\\\" ...\\\\n    <LBRACE_FN> ...\\\\n    \\\\\\"MULTISET\\\\\\" ...\\\\n    \\\\\\"ARRAY\\\\\\" ...\\\\n    \\\\\\"PERIOD\\\\\\" ...\\\\n    \\\\\\"SPECIFIC\\\\\\" ...\\\\n    <IDENTIFIER> ...\\\\n    <QUOTED_IDENTIFIER> ...\\\\n    <BACK_QUOTED_IDENTIFIER> ...\\\\n    <BRACKET_QUOTED_IDENTIFIER> ...\\\\n    <UNICODE_QUOTED_IDENTIFIER> ...\\\\n    \\\\\\"ABS\\\\\\" ...\\\\n    \\\\\\"AVG\\\\\\" ...\\\\n    \\\\\\"CARDINALITY\\\\\\" ...\\\\n    \\\\\\"CHAR_LENGTH\\\\\\" ...\\\\n    \\\\\\"CHARACTER_LENGTH\\\\\\" ...\\\\n    \\\\\\"COALESCE\\\\\\" ...\\\\n    \\\\\\"COLLECT\\\\\\" ...\\\\n    \\\\\\"COVAR_POP\\\\\\" ...\\\\n    \\\\\\"COVAR_SAMP\\\\\\" ...\\\\n    \\\\\\"CUME_DIST\\\\\\" ...\\\\n    \\\\\\"COUNT\\\\\\" ...\\\\n    \\\\\\"CURRENT_DATE\\\\\\" ...\\\\n    \\\\\\"CURRENT_TIME\\\\\\" ...\\\\n    \\\\\\"CURRENT_TIMESTAMP\\\\\\" ...\\\\n    \\\\\\"DENSE_RANK\\\\\\" ...\\\\n    \\\\\\"ELEMENT\\\\\\" ...\\\\n    \\\\\\"EXP\\\\\\" ...\\\\n    \\\\\\"FIRST_VALUE\\\\\\" ...\\\\n    \\\\\\"FUSION\\\\\\" ...\\\\n    \\\\\\"GROUPING\\\\\\" ...\\\\n    \\\\\\"LAG\\\\\\" ...\\\\n    \\\\\\"LEAD\\\\\\" ...\\\\n    \\\\\\"LEFT\\\\\\" ...\\\\n    \\\\\\"LAST_VALUE\\\\\\" ...\\\\n    \\\\\\"LN\\\\\\" ...\\\\n    \\\\\\"LOCALTIME\\\\\\" ...\\\\n    \\\\\\"LOCALTIMESTAMP\\\\\\" ...\\\\n    \\\\\\"LOWER\\\\\\" ...\\\\n    \\\\\\"MAX\\\\\\" ...\\\\n    \\\\\\"MIN\\\\\\" ...\\\\n    \\\\\\"MINUTE\\\\\\" ...\\\\n    \\\\\\"MOD\\\\\\" ...\\\\n    \\\\\\"NTH_VALUE\\\\\\" ...\\\\n    \\\\\\"NTILE\\\\\\" ...\\\\n    \\\\\\"NULLIF\\\\\\" ...\\\\n    \\\\\\"OCTET_LENGTH\\\\\\" ...\\\\n    \\\\\\"PERCENT_RANK\\\\\\" ...\\\\n    \\\\\\"POWER\\\\\\" ...\\\\n    \\\\\\"RANK\\\\\\" ...\\\\n    \\\\\\"REGR_COUNT\\\\\\" ...\\\\n    \\\\\\"REGR_SXX\\\\\\" ...\\\\n    \\\\\\"REGR_SYY\\\\\\" ...\\\\n    \\\\\\"RIGHT\\\\\\" ...\\\\n    \\\\\\"ROW_NUMBER\\\\\\" ...\\\\n    \\\\\\"SECOND\\\\\\" ...\\\\n    \\\\\\"SQRT\\\\\\" ...\\\\n    \\\\\\"STDDEV_POP\\\\\\" ...\\\\n    \\\\\\"STDDEV_SAMP\\\\\\" ...\\\\n    \\\\\\"SUM\\\\\\" ...\\\\n    \\\\\\"UPPER\\\\\\" ...\\\\n    \\\\\\"TRUNCATE\\\\\\" ...\\\\n    \\\\\\"USER\\\\\\" ...\\\\n    \\\\\\"VAR_POP\\\\\\" ...\\\\n    \\\\\\"VAR_SAMP\\\\\\" ...\\\\n    \\\\\\"CURRENT_CATALOG\\\\\\" ...\\\\n    \\\\\\"CURRENT_DEFAULT_TRANSFORM_GROUP\\\\\\" ...\\\\n    \\\\\\"CURRENT_PATH\\\\\\" ...\\\\n    \\\\\\"CURRENT_ROLE\\\\\\" ...\\\\n    \\\\\\"CURRENT_SCHEMA\\\\\\" ...\\\\n    \\\\\\"CURRENT_USER\\\\\\" ...\\\\n    \\\\\\"SESSION_USER\\\\\\" ...\\\\n    \\\\\\"SYSTEM_USER\\\\\\" ...\\\\n    \\\\\\"NEW\\\\\\" ...\\\\n    \\\\\\"CASE\\\\\\" ...\\\\n    \\\\\\"CURRENT\\\\\\" ...\\\\n    \\\\\\"CURSOR\\\\\\" ...\\\\n    \\\\\\"ROW\\\\\\" ...\\\\n    \\\\\\"(\\\\\\" ...\\\\n    \\",\\"errorClass\\":\\"org.apache.calcite.sql.parser.SqlParseException\\",\\"host\\":null}",
            },
          },
          "id": "8c50267b-ca15-4001-8b69-3d3b5c0db932",
          "nativeQuery": undefined,
          "queryContext": undefined,
          "result": undefined,
          "sqlQuery": undefined,
          "stages": undefined,
          "startTime": undefined,
          "status": "FAILED",
          "warnings": undefined,
        }
      `);
    });
  });

  describe('.fromTaskDetail', () => {
    it('fails for bad status (error: null)', () => {
      expect(() =>
        Execution.fromTaskPayloadAndReport(
          {} as any,
          {
            asyncResultId: 'multi-stage-query-sql-1392d806-c17f-4937-94ee-8fa0a3ce1566',
            error: null,
          } as any,
        ),
      ).toThrowError('Invalid payload');
    });

    it('works in a general case', () => {
      expect(
        Execution.fromTaskPayloadAndReport(
          {
            task: 'multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9',
            payload: {
              type: 'query_controller',
              id: 'multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9',
              spec: {
                query: {
                  queryType: 'scan',
                  dataSource: {
                    type: 'external',
                    inputSource: {
                      type: 'http',
                      uris: ['https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz'],
                      httpAuthenticationUsername: null,
                      httpAuthenticationPassword: null,
                    },

                    inputFormat: {
                      type: 'json',
                      flattenSpec: null,
                      featureSpec: {},
                      keepNullColumns: false,
                    },

                    signature: [
                      { name: 'timestamp', type: 'STRING' },
                      { name: 'agent_category', type: 'STRING' },
                    ],
                  },

                  intervals: {
                    type: 'intervals',
                    intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
                  },

                  virtualColumns: [],
                  resultFormat: 'compactedList',
                  batchSize: 20480,
                  limit: 5,
                  filter: null,
                  columns: ['agent_category', 'timestamp'],
                  legacy: false,
                  context: {
                    __userIdentity__: 'allowAll',
                    finalize: true,
                    queryId: 'hello',
                    sqlOuterLimit: 1001,
                    sqlQueryId: 'b275662f-6d9e-4275-b437-533dd9fe9ed9',
                    multiStageQuery: true,
                    msqNumTasks: 2,
                    msqSignature:
                      '[{"name":"agent_category","type":"STRING"},{"name":"timestamp","type":"STRING"}]',
                  },

                  descending: false,
                  granularity: { type: 'all' },
                },

                columnMappings: [
                  { queryColumn: 'timestamp', outputColumn: 'timestamp' },
                  { queryColumn: 'agent_category', outputColumn: 'agent_category' },
                ],

                destination: { type: 'taskReport' },
                tuningConfig: {
                  type: 'index_parallel',
                  maxRowsPerSegment: 3000000,
                  appendableIndexSpec: { type: 'onheap', preserveExistingMetrics: false },
                  maxRowsInMemory: 100000,
                  maxBytesInMemory: 0,
                  skipBytesInMemoryOverheadCheck: false,
                  maxTotalRows: null,
                  numShards: null,
                  splitHintSpec: null,
                  partitionsSpec: {
                    type: 'dynamic',
                    maxRowsPerSegment: 3000000,
                    maxTotalRows: null,
                  },

                  indexSpec: {
                    bitmap: { type: 'roaring', compressRunOnSerialization: true },
                    dimensionCompression: 'lz4',
                    metricCompression: 'lz4',
                    longEncoding: 'longs',
                    segmentLoader: null,
                  },

                  indexSpecForIntermediatePersists: {
                    bitmap: { type: 'roaring', compressRunOnSerialization: true },
                    dimensionCompression: 'lz4',
                    metricCompression: 'lz4',
                    longEncoding: 'longs',
                    segmentLoader: null,
                  },

                  maxPendingPersists: 0,
                  forceGuaranteedRollup: false,
                  reportParseExceptions: false,
                  pushTimeout: 0,
                  segmentWriteOutMediumFactory: null,
                  maxNumConcurrentSubTasks: 2,
                  maxRetry: 1,
                  taskStatusCheckPeriodMs: 1000,
                  chatHandlerTimeout: 'PT10S',
                  chatHandlerNumRetries: 5,
                  maxNumSegmentsToMerge: 100,
                  totalNumMergeTasks: 10,
                  logParseExceptions: false,
                  maxParseExceptions: 2147483647,
                  maxSavedParseExceptions: 0,
                  maxColumnsToMerge: -1,
                  awaitSegmentAvailabilityTimeoutMillis: 0,
                  maxAllowedLockCount: -1,
                  partitionDimensions: [],
                },
              },

              sqlQuery:
                'SELECT *\nFROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"}]\'\n  )\n)\nLIMIT 5',
              sqlQueryContext: {
                __userIdentity__: 'allowAll',
                queryId: 'hello',
                sqlOuterLimit: 1001,
                sqlQueryId: 'b275662f-6d9e-4275-b437-533dd9fe9ed9',
                multiStageQuery: true,
                msqNumTasks: 2,
                msqSignature:
                  '[{"name":"agent_category","type":"STRING"},{"name":"timestamp","type":"STRING"}]',
              },

              sqlTypeNames: ['VARCHAR', 'VARCHAR'],
              context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
              groupId: 'multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9',
              dataSource: '__query_select',
              resource: {
                availabilityGroup: 'multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9',
                requiredCapacity: 1,
              },
            },
          },

          {
            multiStageQuery: {
              taskId: 'multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9',
              payload: {
                status: {
                  status: 'SUCCESS',
                  startTime: '2022-05-12T17:43:47.153Z',
                  durationMs: 1164,
                },

                stages: [
                  {
                    stageNumber: 0,
                    inputStages: [],
                    processorType: 'ScanQueryFrameProcessorFactory',
                    phase: 'FINISHED',
                    workerCount: 2,
                    partitionCount: 1,
                    inputFileCount: 1,
                    startTime: '2022-05-12T17:43:47.515Z',
                    duration: 756,
                    clusterBy: { columns: [] },
                    query: {
                      queryType: 'scan',
                      dataSource: {
                        type: 'external',
                        inputSource: {
                          type: 'http',
                          uris: ['https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz'],
                          httpAuthenticationUsername: null,
                          httpAuthenticationPassword: null,
                        },

                        inputFormat: {
                          type: 'json',
                          flattenSpec: null,
                          featureSpec: {},
                          keepNullColumns: false,
                        },

                        signature: [
                          { name: 'timestamp', type: 'STRING' },
                          { name: 'agent_category', type: 'STRING' },
                        ],
                      },

                      intervals: {
                        type: 'intervals',
                        intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
                      },

                      virtualColumns: [],
                      resultFormat: 'compactedList',
                      batchSize: 20480,
                      limit: 5,
                      filter: null,
                      columns: ['agent_category', 'timestamp'],
                      legacy: false,
                      context: {
                        __userIdentity__: 'allowAll',
                        finalize: true,
                        queryId: 'hello',
                        sqlOuterLimit: 1001,
                        sqlQueryId: 'b275662f-6d9e-4275-b437-533dd9fe9ed9',
                        multiStageQuery: true,
                        msqNumTasks: 2,
                        msqSignature:
                          '[{"name":"agent_category","type":"STRING"},{"name":"timestamp","type":"STRING"}]',
                      },

                      descending: false,
                      granularity: { type: 'all' },
                    },
                  },

                  {
                    stageNumber: 1,
                    inputStages: [0],
                    processorType: 'OffsetLimitFrameProcessorFactory',
                    phase: 'FINISHED',
                    workerCount: 1,
                    partitionCount: 1,
                    startTime: '2022-05-12T17:43:48.266Z',
                    duration: 51,
                    clusterBy: { columns: [] },
                  },
                ],

                counters: [
                  {
                    workerNumber: 0,
                    counters: {
                      inputStageChannel: [
                        {
                          stageNumber: 1,
                          partitionNumber: 0,
                          frames: 1,
                          rows: 81920,
                          bytes: 4483872,
                        },
                      ],

                      inputExternal: [
                        {
                          stageNumber: 0,
                          partitionNumber: -1,
                          frames: 0,
                          rows: 81921,
                          bytes: 0,
                          files: 1,
                        },
                      ],

                      processor: [
                        {
                          stageNumber: 0,
                          partitionNumber: -1,
                          frames: 1,
                          rows: 81920,
                          bytes: 4483872,
                        },

                        { stageNumber: 1, partitionNumber: 0, frames: 1, rows: 5, bytes: 309 },
                      ],

                      sort: [
                        {
                          stageNumber: 0,
                          partitionNumber: 0,
                          frames: 1,
                          rows: 81920,
                          bytes: 4483872,
                        },

                        { stageNumber: 1, partitionNumber: 0, frames: 1, rows: 5, bytes: 309 },
                      ],
                    },

                    sortProgress: [],
                  },

                  {
                    workerNumber: 1,
                    counters: {
                      sort: [{ stageNumber: 0, partitionNumber: 0, frames: 0, rows: 0, bytes: 0 }],
                    },

                    sortProgress: [],
                  },
                ],

                results: {
                  signature: [
                    { name: 'timestamp', type: 'STRING' },
                    { name: 'agent_category', type: 'STRING' },
                  ],

                  sqlTypeNames: ['VARCHAR', 'VARCHAR'],
                  results: [
                    ['2019-08-25T00:00:00.031Z', 'Personal computer'],
                    ['2019-08-25T00:00:00.059Z', 'Smartphone'],
                    ['2019-08-25T00:00:00.178Z', 'Personal computer'],
                    ['2019-08-25T00:00:00.965Z', 'Personal computer'],
                    ['2019-08-25T00:00:01.241Z', 'Smartphone'],
                  ],
                },
              },
            },
          },
        ),
      ).toMatchInlineSnapshot(`
        Execution {
          "_payload": Object {
            "payload": Object {
              "context": Object {
                "forceTimeChunkLock": true,
                "useLineageBasedSegmentAllocation": true,
              },
              "dataSource": "__query_select",
              "groupId": "multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9",
              "id": "multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9",
              "resource": Object {
                "availabilityGroup": "multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9",
                "requiredCapacity": 1,
              },
              "spec": Object {
                "columnMappings": Array [
                  Object {
                    "outputColumn": "timestamp",
                    "queryColumn": "timestamp",
                  },
                  Object {
                    "outputColumn": "agent_category",
                    "queryColumn": "agent_category",
                  },
                ],
                "destination": Object {
                  "type": "taskReport",
                },
                "query": Object {
                  "batchSize": 20480,
                  "columns": Array [
                    "agent_category",
                    "timestamp",
                  ],
                  "context": Object {
                    "__userIdentity__": "allowAll",
                    "finalize": true,
                    "msqNumTasks": 2,
                    "msqSignature": "[{\\"name\\":\\"agent_category\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"STRING\\"}]",
                    "multiStageQuery": true,
                    "queryId": "hello",
                    "sqlOuterLimit": 1001,
                    "sqlQueryId": "b275662f-6d9e-4275-b437-533dd9fe9ed9",
                  },
                  "dataSource": Object {
                    "inputFormat": Object {
                      "featureSpec": Object {},
                      "flattenSpec": null,
                      "keepNullColumns": false,
                      "type": "json",
                    },
                    "inputSource": Object {
                      "httpAuthenticationPassword": null,
                      "httpAuthenticationUsername": null,
                      "type": "http",
                      "uris": Array [
                        "https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz",
                      ],
                    },
                    "signature": Array [
                      Object {
                        "name": "timestamp",
                        "type": "STRING",
                      },
                      Object {
                        "name": "agent_category",
                        "type": "STRING",
                      },
                    ],
                    "type": "external",
                  },
                  "descending": false,
                  "filter": null,
                  "granularity": Object {
                    "type": "all",
                  },
                  "intervals": Object {
                    "intervals": Array [
                      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                    ],
                    "type": "intervals",
                  },
                  "legacy": false,
                  "limit": 5,
                  "queryType": "scan",
                  "resultFormat": "compactedList",
                  "virtualColumns": Array [],
                },
                "tuningConfig": Object {
                  "appendableIndexSpec": Object {
                    "preserveExistingMetrics": false,
                    "type": "onheap",
                  },
                  "awaitSegmentAvailabilityTimeoutMillis": 0,
                  "chatHandlerNumRetries": 5,
                  "chatHandlerTimeout": "PT10S",
                  "forceGuaranteedRollup": false,
                  "indexSpec": Object {
                    "bitmap": Object {
                      "compressRunOnSerialization": true,
                      "type": "roaring",
                    },
                    "dimensionCompression": "lz4",
                    "longEncoding": "longs",
                    "metricCompression": "lz4",
                    "segmentLoader": null,
                  },
                  "indexSpecForIntermediatePersists": Object {
                    "bitmap": Object {
                      "compressRunOnSerialization": true,
                      "type": "roaring",
                    },
                    "dimensionCompression": "lz4",
                    "longEncoding": "longs",
                    "metricCompression": "lz4",
                    "segmentLoader": null,
                  },
                  "logParseExceptions": false,
                  "maxAllowedLockCount": -1,
                  "maxBytesInMemory": 0,
                  "maxColumnsToMerge": -1,
                  "maxNumConcurrentSubTasks": 2,
                  "maxNumSegmentsToMerge": 100,
                  "maxParseExceptions": 2147483647,
                  "maxPendingPersists": 0,
                  "maxRetry": 1,
                  "maxRowsInMemory": 100000,
                  "maxRowsPerSegment": 3000000,
                  "maxSavedParseExceptions": 0,
                  "maxTotalRows": null,
                  "numShards": null,
                  "partitionDimensions": Array [],
                  "partitionsSpec": Object {
                    "maxRowsPerSegment": 3000000,
                    "maxTotalRows": null,
                    "type": "dynamic",
                  },
                  "pushTimeout": 0,
                  "reportParseExceptions": false,
                  "segmentWriteOutMediumFactory": null,
                  "skipBytesInMemoryOverheadCheck": false,
                  "splitHintSpec": null,
                  "taskStatusCheckPeriodMs": 1000,
                  "totalNumMergeTasks": 10,
                  "type": "index_parallel",
                },
              },
              "sqlQuery": "SELECT *
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"}]'
          )
        )
        LIMIT 5",
              "sqlQueryContext": Object {
                "__userIdentity__": "allowAll",
                "msqNumTasks": 2,
                "msqSignature": "[{\\"name\\":\\"agent_category\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"STRING\\"}]",
                "multiStageQuery": true,
                "queryId": "hello",
                "sqlOuterLimit": 1001,
                "sqlQueryId": "b275662f-6d9e-4275-b437-533dd9fe9ed9",
              },
              "sqlTypeNames": Array [
                "VARCHAR",
                "VARCHAR",
              ],
              "type": "query_controller",
            },
            "task": "multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9",
          },
          "destination": Object {
            "type": "taskReport",
          },
          "duration": 1164,
          "engine": "sql-task",
          "error": undefined,
          "id": "multi-stage-query-sql-b275662f-6d9e-4275-b437-533dd9fe9ed9",
          "nativeQuery": Object {
            "batchSize": 20480,
            "columns": Array [
              "agent_category",
              "timestamp",
            ],
            "context": Object {
              "__userIdentity__": "allowAll",
              "finalize": true,
              "msqNumTasks": 2,
              "msqSignature": "[{\\"name\\":\\"agent_category\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"STRING\\"}]",
              "multiStageQuery": true,
              "queryId": "hello",
              "sqlOuterLimit": 1001,
              "sqlQueryId": "b275662f-6d9e-4275-b437-533dd9fe9ed9",
            },
            "dataSource": Object {
              "inputFormat": Object {
                "featureSpec": Object {},
                "flattenSpec": null,
                "keepNullColumns": false,
                "type": "json",
              },
              "inputSource": Object {
                "httpAuthenticationPassword": null,
                "httpAuthenticationUsername": null,
                "type": "http",
                "uris": Array [
                  "https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz",
                ],
              },
              "signature": Array [
                Object {
                  "name": "timestamp",
                  "type": "STRING",
                },
                Object {
                  "name": "agent_category",
                  "type": "STRING",
                },
              ],
              "type": "external",
            },
            "descending": false,
            "filter": null,
            "granularity": Object {
              "type": "all",
            },
            "intervals": Object {
              "intervals": Array [
                "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
              ],
              "type": "intervals",
            },
            "legacy": false,
            "limit": 5,
            "queryType": "scan",
            "resultFormat": "compactedList",
            "virtualColumns": Array [],
          },
          "queryContext": Object {
            "__userIdentity__": "allowAll",
            "msqNumTasks": 2,
            "multiStageQuery": true,
            "sqlOuterLimit": 1001,
          },
          "result": QueryResult {
            "header": Array [
              Column {
                "name": "timestamp",
                "nativeType": "STRING",
                "sqlType": "VARCHAR",
              },
              Column {
                "name": "agent_category",
                "nativeType": "STRING",
                "sqlType": "VARCHAR",
              },
            ],
            "query": Object {
              "context": Object {
                "__userIdentity__": "allowAll",
                "msqNumTasks": 2,
                "multiStageQuery": true,
                "sqlOuterLimit": 1001,
              },
            },
            "queryDuration": undefined,
            "queryId": undefined,
            "resultContext": undefined,
            "rows": Array [
              Array [
                "2019-08-25T00:00:00.031Z",
                "Personal computer",
              ],
              Array [
                "2019-08-25T00:00:00.059Z",
                "Smartphone",
              ],
              Array [
                "2019-08-25T00:00:00.178Z",
                "Personal computer",
              ],
              Array [
                "2019-08-25T00:00:00.965Z",
                "Personal computer",
              ],
              Array [
                "2019-08-25T00:00:01.241Z",
                "Smartphone",
              ],
            ],
            "sqlQuery": SqlQuery {
              "clusteredByClause": undefined,
              "decorator": undefined,
              "explainClause": undefined,
              "fromClause": SqlFromClause {
                "expressions": SeparatedArray {
                  "separators": Array [],
                  "values": Array [
                    SqlFunction {
                      "args": SeparatedArray {
                        "separators": Array [],
                        "values": Array [
                          SqlFunction {
                            "args": SeparatedArray {
                              "separators": Array [
                                Separator {
                                  "left": "",
                                  "right": "
            ",
                                  "separator": ",",
                                },
                                Separator {
                                  "left": "",
                                  "right": "
            ",
                                  "separator": ",",
                                },
                              ],
                              "values": Array [
                                SqlLiteral {
                                  "keywords": Object {},
                                  "spacing": Object {},
                                  "stringValue": "'{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}'",
                                  "type": "literal",
                                  "value": "{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}",
                                },
                                SqlLiteral {
                                  "keywords": Object {},
                                  "spacing": Object {},
                                  "stringValue": "'{\\"type\\":\\"json\\"}'",
                                  "type": "literal",
                                  "value": "{\\"type\\":\\"json\\"}",
                                },
                                SqlLiteral {
                                  "keywords": Object {},
                                  "spacing": Object {},
                                  "stringValue": "'[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"}]'",
                                  "type": "literal",
                                  "value": "[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"}]",
                                },
                              ],
                            },
                            "decorator": undefined,
                            "functionName": "EXTERN",
                            "keywords": Object {
                              "functionName": "EXTERN",
                            },
                            "spacing": Object {
                              "postArguments": "
          ",
                              "postLeftParen": "
            ",
                              "preLeftParen": "",
                            },
                            "specialParen": undefined,
                            "type": "function",
                            "whereClause": undefined,
                          },
                        ],
                      },
                      "decorator": undefined,
                      "functionName": "TABLE",
                      "keywords": Object {
                        "functionName": "TABLE",
                      },
                      "spacing": Object {
                        "postArguments": "
        ",
                        "postLeftParen": "
          ",
                        "preLeftParen": "",
                      },
                      "specialParen": undefined,
                      "type": "function",
                      "whereClause": undefined,
                    },
                  ],
                },
                "joinParts": undefined,
                "keywords": Object {
                  "from": "FROM",
                },
                "spacing": Object {
                  "postFrom": " ",
                },
                "type": "fromClause",
              },
              "groupByClause": undefined,
              "havingClause": undefined,
              "insertClause": undefined,
              "keywords": Object {
                "select": "SELECT",
              },
              "limitClause": SqlLimitClause {
                "keywords": Object {
                  "limit": "LIMIT",
                },
                "limit": SqlLiteral {
                  "keywords": Object {},
                  "spacing": Object {},
                  "stringValue": "5",
                  "type": "literal",
                  "value": 5,
                },
                "spacing": Object {
                  "postLimit": " ",
                },
                "type": "limitClause",
              },
              "offsetClause": undefined,
              "orderByClause": undefined,
              "partitionedByClause": undefined,
              "replaceClause": undefined,
              "selectExpressions": SeparatedArray {
                "separators": Array [],
                "values": Array [
                  SqlStar {
                    "keywords": Object {},
                    "namespaceRefName": undefined,
                    "spacing": Object {},
                    "tableRefName": undefined,
                    "type": "star",
                  },
                ],
              },
              "spacing": Object {
                "postSelect": " ",
                "preFromClause": "
        ",
                "preLimitClause": "
        ",
              },
              "type": "query",
              "unionQuery": undefined,
              "whereClause": undefined,
              "withClause": undefined,
            },
            "sqlQueryId": undefined,
          },
          "sqlQuery": "SELECT *
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"}]'
          )
        )
        LIMIT 5",
          "stages": Stages {
            "counters": Array [
              Object {
                "counters": Object {
                  "inputExternal": Array [
                    Object {
                      "bytes": 0,
                      "files": 1,
                      "frames": 0,
                      "partitionNumber": -1,
                      "rows": 81921,
                      "stageNumber": 0,
                    },
                  ],
                  "inputStageChannel": Array [
                    Object {
                      "bytes": 4483872,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 81920,
                      "stageNumber": 1,
                    },
                  ],
                  "processor": Array [
                    Object {
                      "bytes": 4483872,
                      "frames": 1,
                      "partitionNumber": -1,
                      "rows": 81920,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 309,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 5,
                      "stageNumber": 1,
                    },
                  ],
                  "sort": Array [
                    Object {
                      "bytes": 4483872,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 81920,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 309,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 5,
                      "stageNumber": 1,
                    },
                  ],
                },
                "sortProgress": Array [],
                "workerNumber": 0,
              },
              Object {
                "counters": Object {
                  "sort": Array [
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": 0,
                      "rows": 0,
                      "stageNumber": 0,
                    },
                  ],
                },
                "sortProgress": Array [],
                "workerNumber": 1,
              },
            ],
            "stages": Array [
              Object {
                "clusterBy": Object {
                  "columns": Array [],
                },
                "duration": 756,
                "inputFileCount": 1,
                "inputStages": Array [],
                "partitionCount": 1,
                "phase": "FINISHED",
                "processorType": "ScanQueryFrameProcessorFactory",
                "query": Object {
                  "batchSize": 20480,
                  "columns": Array [
                    "agent_category",
                    "timestamp",
                  ],
                  "context": Object {
                    "__userIdentity__": "allowAll",
                    "finalize": true,
                    "msqNumTasks": 2,
                    "msqSignature": "[{\\"name\\":\\"agent_category\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"STRING\\"}]",
                    "multiStageQuery": true,
                    "queryId": "hello",
                    "sqlOuterLimit": 1001,
                    "sqlQueryId": "b275662f-6d9e-4275-b437-533dd9fe9ed9",
                  },
                  "dataSource": Object {
                    "inputFormat": Object {
                      "featureSpec": Object {},
                      "flattenSpec": null,
                      "keepNullColumns": false,
                      "type": "json",
                    },
                    "inputSource": Object {
                      "httpAuthenticationPassword": null,
                      "httpAuthenticationUsername": null,
                      "type": "http",
                      "uris": Array [
                        "https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz",
                      ],
                    },
                    "signature": Array [
                      Object {
                        "name": "timestamp",
                        "type": "STRING",
                      },
                      Object {
                        "name": "agent_category",
                        "type": "STRING",
                      },
                    ],
                    "type": "external",
                  },
                  "descending": false,
                  "filter": null,
                  "granularity": Object {
                    "type": "all",
                  },
                  "intervals": Object {
                    "intervals": Array [
                      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                    ],
                    "type": "intervals",
                  },
                  "legacy": false,
                  "limit": 5,
                  "queryType": "scan",
                  "resultFormat": "compactedList",
                  "virtualColumns": Array [],
                },
                "stageNumber": 0,
                "startTime": "2022-05-12T17:43:47.515Z",
                "workerCount": 2,
              },
              Object {
                "clusterBy": Object {
                  "columns": Array [],
                },
                "duration": 51,
                "inputStages": Array [
                  0,
                ],
                "partitionCount": 1,
                "phase": "FINISHED",
                "processorType": "OffsetLimitFrameProcessorFactory",
                "stageNumber": 1,
                "startTime": "2022-05-12T17:43:48.266Z",
                "workerCount": 1,
              },
            ],
          },
          "startTime": 2022-05-12T17:43:47.153Z,
          "status": "SUCCESS",
          "warnings": undefined,
        }
      `);
    });
  });
});
