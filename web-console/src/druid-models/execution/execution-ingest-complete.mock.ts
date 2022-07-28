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

/*
For query:

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

export const EXECUTION_INGEST_COMPLETE = Execution.fromTaskPayloadAndReport(
  {
    task: 'query-c1b808f3-ebad-4773-906c-63ff376ed50f',
    payload: {
      type: 'query_controller',
      id: 'query-c1b808f3-ebad-4773-906c-63ff376ed50f',
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
            __timeColumn: 'v0',
            finalize: false,
            finalizeAggregations: false,
            groupByEnableMultiValueUnnesting: false,
            maxNumTasks: 3,
            msqSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
            multiStageQuery: true,
            sqlInsertSegmentGranularity: '{"type":"all"}',
            sqlQueryId: 'c1b808f3-ebad-4773-906c-63ff376ed50f',
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
          partitionsSpec: { type: 'dynamic', maxRowsPerSegment: 3000000, maxTotalRows: null },
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
        finalizeAggregations: false,
        groupByEnableMultiValueUnnesting: false,
        maxNumTasks: 3,
        maxParseExceptions: 0,
        msqSignature: '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
        multiStageQuery: true,
        sqlInsertSegmentGranularity: '{"type":"all"}',
        sqlQueryId: 'c1b808f3-ebad-4773-906c-63ff376ed50f',
        sqlReplaceTimeChunks: 'all',
      },
      sqlTypeNames: ['TIMESTAMP', 'VARCHAR'],
      context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
      groupId: 'query-c1b808f3-ebad-4773-906c-63ff376ed50f',
      dataSource: 'kttm_simple',
      resource: {
        availabilityGroup: 'query-c1b808f3-ebad-4773-906c-63ff376ed50f',
        requiredCapacity: 1,
      },
    },
  },

  {
    multiStageQuery: {
      taskId: 'query-c1b808f3-ebad-4773-906c-63ff376ed50f',
      payload: {
        status: {
          status: 'SUCCESS',
          startTime: '2022-07-26T19:36:05.044Z',
          durationMs: 9053,
          warningReports: [],
        },
        stages: [
          {
            stageNumber: 0,
            definition: {
              id: 'c0365a7c-9dce-4f4d-bec9-2da6a8991e5e_0',
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
                    __timeColumn: 'v0',
                    finalize: false,
                    finalizeAggregations: false,
                    groupByEnableMultiValueUnnesting: false,
                    maxNumTasks: 3,
                    msqSignature:
                      '[{"name":"agent_type","type":"STRING"},{"name":"v0","type":"LONG"}]',
                    multiStageQuery: true,
                    sqlInsertSegmentGranularity: '{"type":"all"}',
                    sqlQueryId: 'c1b808f3-ebad-4773-906c-63ff376ed50f',
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
            phase: 'FINISHED',
            workerCount: 1,
            partitionCount: 1,
            startTime: '2022-07-26T19:36:07.262Z',
            duration: 5132,
            sort: true,
          },
          {
            stageNumber: 1,
            definition: {
              id: 'c0365a7c-9dce-4f4d-bec9-2da6a8991e5e_1',
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
                    intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
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
            phase: 'FINISHED',
            workerCount: 1,
            partitionCount: 1,
            startTime: '2022-07-26T19:36:12.391Z',
            duration: 1706,
          },
        ],
        counters: {
          '0': {
            '0': {
              input0: { type: 'channel', rows: [465346], files: [1], totalFiles: [1] },
              output: { type: 'channel', rows: [465346], bytes: [25430674], frames: [4] },
              sort: { type: 'channel', rows: [465346], bytes: [23570446], frames: [38] },
              sortProgress: {
                type: 'sortProgress',
                totalMergingLevels: 3,
                levelToTotalBatches: { '0': 2, '1': 1, '2': 1 },
                levelToMergedBatches: { '0': 2, '1': 1, '2': 1 },
                totalMergersForUltimateLevel: 1,
                progressDigest: 1.0,
              },
            },
          },
          '1': {
            '0': {
              input0: { type: 'channel', rows: [465346], bytes: [23570446], frames: [38] },
            },
          },
        },
      },
    },
  },
);
