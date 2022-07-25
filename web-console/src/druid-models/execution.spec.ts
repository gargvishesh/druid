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
      /* For query:

        REPLACE INTO "kttm_simple" OVERWRITE ALL
        SELECT TIME_PARSE("timestamp") AS "__time", agent_type
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}',
            '[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]'
          )
        )
        PARTITIONED BY ALL TIME
       */

      const execution = Execution.fromTaskPayloadAndReport(
        {
          task: 'query-824ead63-230a-4cf7-b415-3ededa419cb0',
          payload: {
            type: 'query_controller',
            id: 'query-824ead63-230a-4cf7-b415-3ededa419cb0',
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
                    { name: 'agent_type', type: 'STRING' },
                  ],
                },
                intervals: {
                  type: 'intervals',
                  intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
                },
                virtualColumns: [
                  {
                    type: 'expression',
                    name: 'v0',
                    expression: 'timestamp_parse("timestamp",null,\'UTC\')',
                    outputType: 'LONG',
                  },
                ],
                resultFormat: 'compactedList',
                columns: ['agent_type', 'v0'],
                legacy: false,
                context: {
                  finalize: false,
                  groupByEnableMultiValueUnnesting: false,
                  msqFinalizeAggregations: false,
                  msqMaxNumTasks: 3,
                  msqSignature:
                    '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                  msqTimeColumn: 'v0',
                  multiStageQuery: true,
                  sqlInsertSegmentGranularity: '{"type":"all"}',
                  sqlQueryId: '824ead63-230a-4cf7-b415-3ededa419cb0',
                  sqlReplaceTimeChunks: 'all',
                },
                granularity: { type: 'all' },
              },
              columnMappings: [
                { queryColumn: 'v0', outputColumn: '__time' },
                { queryColumn: 'agent_type', outputColumn: 'agent_type' },
              ],
              destination: {
                type: 'dataSource',
                dataSource: 'kttm_simple',
                segmentGranularity: { type: 'all' },
                replaceTimeChunks: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
              },
              assignmentStrategy: 'max',
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
              'REPLACE INTO "kttm_simple" OVERWRITE ALL\nSELECT TIME_PARSE("timestamp") AS "__time", agent_type\nFROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_type","type":"string"}]\'\n  )\n)\nPARTITIONED BY ALL TIME',
            sqlQueryContext: {
              groupByEnableMultiValueUnnesting: false,
              maxParseExceptions: 0,
              msqFinalizeAggregations: false,
              msqMaxNumTasks: 3,
              msqSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
              multiStageQuery: true,
              sqlInsertSegmentGranularity: '{"type":"all"}',
              sqlQueryId: '824ead63-230a-4cf7-b415-3ededa419cb0',
              sqlReplaceTimeChunks: 'all',
            },
            sqlTypeNames: ['TIMESTAMP', 'VARCHAR'],
            context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
            groupId: 'query-824ead63-230a-4cf7-b415-3ededa419cb0',
            dataSource: 'kttm_simple',
            resource: {
              availabilityGroup: 'query-824ead63-230a-4cf7-b415-3ededa419cb0',
              requiredCapacity: 1,
            },
          },
        },

        {
          multiStageQuery: {
            taskId: 'query-824ead63-230a-4cf7-b415-3ededa419cb0',
            payload: {
              status: {
                status: 'RUNNING',
                startTime: '2022-07-19T05:24:26.846Z',
                durationMs: 484,
                warningReports: [],
              },
              stages: [
                {
                  stageNumber: 0,
                  definition: {
                    id: '801eef28-210d-44a9-966b-1988f36fc9a2_0',
                    input: [
                      {
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
                          { name: 'agent_type', type: 'STRING' },
                        ],
                      },
                    ],
                    processor: {
                      type: 'scan',
                      query: {
                        queryType: 'scan',
                        dataSource: { type: 'inputNumber', inputNumber: 0 },
                        intervals: {
                          type: 'intervals',
                          intervals: [
                            '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
                          ],
                        },
                        virtualColumns: [
                          {
                            type: 'expression',
                            name: 'v0',
                            expression: 'timestamp_parse("timestamp",null,\'UTC\')',
                            outputType: 'LONG',
                          },
                        ],
                        resultFormat: 'compactedList',
                        columns: ['agent_type', 'v0'],
                        legacy: false,
                        context: {
                          finalize: false,
                          groupByEnableMultiValueUnnesting: false,
                          msqFinalizeAggregations: false,
                          msqMaxNumTasks: 3,
                          msqSignature:
                            '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                          msqTimeColumn: 'v0',
                          multiStageQuery: true,
                          sqlInsertSegmentGranularity: '{"type":"all"}',
                          sqlQueryId: '824ead63-230a-4cf7-b415-3ededa419cb0',
                          sqlReplaceTimeChunks: 'all',
                        },
                        granularity: { type: 'all' },
                      },
                    },
                    signature: [
                      { name: '__boost', type: 'LONG' },
                      { name: 'agent_type', type: 'STRING' },
                      { name: 'v0', type: 'LONG' },
                    ],
                    shuffleSpec: {
                      type: 'targetSize',
                      clusterBy: { columns: [{ columnName: '__boost' }] },
                      targetSize: 3000000,
                      aggregate: false,
                    },
                    maxWorkerCount: 2,
                    shuffleCheckHasMultipleValues: true,
                  },
                  sort: true,
                },
                {
                  stageNumber: 1,
                  definition: {
                    id: '801eef28-210d-44a9-966b-1988f36fc9a2_1',
                    input: [{ type: 'stage', stage: 0 }],
                    processor: {
                      type: 'segmentGenerator',
                      dataSchema: {
                        dataSource: 'kttm_simple',
                        timestampSpec: { column: '__time', format: 'millis', missingValue: null },
                        dimensionsSpec: {
                          dimensions: [
                            {
                              type: 'string',
                              name: 'agent_type',
                              multiValueHandling: 'SORTED_ARRAY',
                              createBitmapIndex: true,
                            },
                          ],
                          dimensionExclusions: ['__time'],
                          includeAllDimensions: false,
                        },
                        metricsSpec: [],
                        granularitySpec: {
                          type: 'arbitrary',
                          queryGranularity: { type: 'none' },
                          rollup: false,
                          intervals: [
                            '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
                          ],
                        },
                        transformSpec: { filter: null, transforms: [] },
                      },
                      columnMappings: [
                        { queryColumn: 'v0', outputColumn: '__time' },
                        { queryColumn: 'agent_type', outputColumn: 'agent_type' },
                      ],
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
                    signature: [],
                    maxWorkerCount: 2,
                  },
                },
              ],
              counters: {},
            },
          },
        },
      );

      expect(execution).toMatchInlineSnapshot(`
        Execution {
          "_payload": Object {
            "payload": Object {
              "context": Object {
                "forceTimeChunkLock": true,
                "useLineageBasedSegmentAllocation": true,
              },
              "dataSource": "kttm_simple",
              "groupId": "query-824ead63-230a-4cf7-b415-3ededa419cb0",
              "id": "query-824ead63-230a-4cf7-b415-3ededa419cb0",
              "resource": Object {
                "availabilityGroup": "query-824ead63-230a-4cf7-b415-3ededa419cb0",
                "requiredCapacity": 1,
              },
              "spec": Object {
                "assignmentStrategy": "max",
                "columnMappings": Array [
                  Object {
                    "outputColumn": "__time",
                    "queryColumn": "v0",
                  },
                  Object {
                    "outputColumn": "agent_type",
                    "queryColumn": "agent_type",
                  },
                ],
                "destination": Object {
                  "dataSource": "kttm_simple",
                  "replaceTimeChunks": Array [
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                  ],
                  "segmentGranularity": Object {
                    "type": "all",
                  },
                  "type": "dataSource",
                },
                "query": Object {
                  "columns": Array [
                    "agent_type",
                    "v0",
                  ],
                  "context": Object {
                    "finalize": false,
                    "groupByEnableMultiValueUnnesting": false,
                    "msqFinalizeAggregations": false,
                    "msqMaxNumTasks": 3,
                    "msqSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                    "msqTimeColumn": "v0",
                    "multiStageQuery": true,
                    "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                    "sqlQueryId": "824ead63-230a-4cf7-b415-3ededa419cb0",
                    "sqlReplaceTimeChunks": "all",
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
                        "name": "agent_type",
                        "type": "STRING",
                      },
                    ],
                    "type": "external",
                  },
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
                  "queryType": "scan",
                  "resultFormat": "compactedList",
                  "virtualColumns": Array [
                    Object {
                      "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                      "name": "v0",
                      "outputType": "LONG",
                      "type": "expression",
                    },
                  ],
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
              "sqlQuery": "REPLACE INTO \\"kttm_simple\\" OVERWRITE ALL
        SELECT TIME_PARSE(\\"timestamp\\") AS \\"__time\\", agent_type
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"}]'
          )
        )
        PARTITIONED BY ALL TIME",
              "sqlQueryContext": Object {
                "groupByEnableMultiValueUnnesting": false,
                "maxParseExceptions": 0,
                "msqFinalizeAggregations": false,
                "msqMaxNumTasks": 3,
                "msqSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                "multiStageQuery": true,
                "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                "sqlQueryId": "824ead63-230a-4cf7-b415-3ededa419cb0",
                "sqlReplaceTimeChunks": "all",
              },
              "sqlTypeNames": Array [
                "TIMESTAMP",
                "VARCHAR",
              ],
              "type": "query_controller",
            },
            "task": "query-824ead63-230a-4cf7-b415-3ededa419cb0",
          },
          "destination": Object {
            "dataSource": "kttm_simple",
            "replaceTimeChunks": Array [
              "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
            ],
            "segmentGranularity": Object {
              "type": "all",
            },
            "type": "dataSource",
          },
          "duration": 484,
          "engine": "sql-task",
          "error": undefined,
          "id": "query-824ead63-230a-4cf7-b415-3ededa419cb0",
          "nativeQuery": Object {
            "columns": Array [
              "agent_type",
              "v0",
            ],
            "context": Object {
              "finalize": false,
              "groupByEnableMultiValueUnnesting": false,
              "msqFinalizeAggregations": false,
              "msqMaxNumTasks": 3,
              "msqSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
              "msqTimeColumn": "v0",
              "multiStageQuery": true,
              "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
              "sqlQueryId": "824ead63-230a-4cf7-b415-3ededa419cb0",
              "sqlReplaceTimeChunks": "all",
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
                  "name": "agent_type",
                  "type": "STRING",
                },
              ],
              "type": "external",
            },
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
            "queryType": "scan",
            "resultFormat": "compactedList",
            "virtualColumns": Array [
              Object {
                "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                "name": "v0",
                "outputType": "LONG",
                "type": "expression",
              },
            ],
          },
          "queryContext": Object {
            "groupByEnableMultiValueUnnesting": false,
            "maxParseExceptions": 0,
            "msqFinalizeAggregations": false,
            "msqMaxNumTasks": 3,
            "multiStageQuery": true,
          },
          "result": undefined,
          "sqlQuery": "REPLACE INTO \\"kttm_simple\\" OVERWRITE ALL
        SELECT TIME_PARSE(\\"timestamp\\") AS \\"__time\\", agent_type
        FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"}]'
          )
        )
        PARTITIONED BY ALL TIME",
          "stages": Stages {
            "counters": Object {},
            "stages": Array [
              Object {
                "definition": Object {
                  "id": "801eef28-210d-44a9-966b-1988f36fc9a2_0",
                  "input": Array [
                    Object {
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
                          "name": "agent_type",
                          "type": "STRING",
                        },
                      ],
                      "type": "external",
                    },
                  ],
                  "maxWorkerCount": 2,
                  "processor": Object {
                    "query": Object {
                      "columns": Array [
                        "agent_type",
                        "v0",
                      ],
                      "context": Object {
                        "finalize": false,
                        "groupByEnableMultiValueUnnesting": false,
                        "msqFinalizeAggregations": false,
                        "msqMaxNumTasks": 3,
                        "msqSignature": "[{\\"name\\":\\"agent_type\\",\\"type\\":\\"STRING\\"},{\\"name\\":\\"v0\\",\\"type\\":\\"LONG\\"}]",
                        "msqTimeColumn": "v0",
                        "multiStageQuery": true,
                        "sqlInsertSegmentGranularity": "{\\"type\\":\\"all\\"}",
                        "sqlQueryId": "824ead63-230a-4cf7-b415-3ededa419cb0",
                        "sqlReplaceTimeChunks": "all",
                      },
                      "dataSource": Object {
                        "inputNumber": 0,
                        "type": "inputNumber",
                      },
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
                      "queryType": "scan",
                      "resultFormat": "compactedList",
                      "virtualColumns": Array [
                        Object {
                          "expression": "timestamp_parse(\\"timestamp\\",null,'UTC')",
                          "name": "v0",
                          "outputType": "LONG",
                          "type": "expression",
                        },
                      ],
                    },
                    "type": "scan",
                  },
                  "shuffleCheckHasMultipleValues": true,
                  "shuffleSpec": Object {
                    "aggregate": false,
                    "clusterBy": Object {
                      "columns": Array [
                        Object {
                          "columnName": "__boost",
                        },
                      ],
                    },
                    "targetSize": 3000000,
                    "type": "targetSize",
                  },
                  "signature": Array [
                    Object {
                      "name": "__boost",
                      "type": "LONG",
                    },
                    Object {
                      "name": "agent_type",
                      "type": "STRING",
                    },
                    Object {
                      "name": "v0",
                      "type": "LONG",
                    },
                  ],
                },
                "sort": true,
                "stageNumber": 0,
              },
              Object {
                "definition": Object {
                  "id": "801eef28-210d-44a9-966b-1988f36fc9a2_1",
                  "input": Array [
                    Object {
                      "stage": 0,
                      "type": "stage",
                    },
                  ],
                  "maxWorkerCount": 2,
                  "processor": Object {
                    "columnMappings": Array [
                      Object {
                        "outputColumn": "__time",
                        "queryColumn": "v0",
                      },
                      Object {
                        "outputColumn": "agent_type",
                        "queryColumn": "agent_type",
                      },
                    ],
                    "dataSchema": Object {
                      "dataSource": "kttm_simple",
                      "dimensionsSpec": Object {
                        "dimensionExclusions": Array [
                          "__time",
                        ],
                        "dimensions": Array [
                          Object {
                            "createBitmapIndex": true,
                            "multiValueHandling": "SORTED_ARRAY",
                            "name": "agent_type",
                            "type": "string",
                          },
                        ],
                        "includeAllDimensions": false,
                      },
                      "granularitySpec": Object {
                        "intervals": Array [
                          "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
                        ],
                        "queryGranularity": Object {
                          "type": "none",
                        },
                        "rollup": false,
                        "type": "arbitrary",
                      },
                      "metricsSpec": Array [],
                      "timestampSpec": Object {
                        "column": "__time",
                        "format": "millis",
                        "missingValue": null,
                      },
                      "transformSpec": Object {
                        "filter": null,
                        "transforms": Array [],
                      },
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
                    "type": "segmentGenerator",
                  },
                  "signature": Array [],
                },
                "stageNumber": 1,
              },
            ],
          },
          "startTime": 2022-07-19T05:24:26.846Z,
          "status": "RUNNING",
          "warnings": undefined,
        }
      `);
    });
  });
});
