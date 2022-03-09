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

import { QueryExecution } from './query-execution';

describe('QueryExecution', () => {
  describe('.fromAsyncStatus', () => {
    it('works', () => {
      expect(
        QueryExecution.fromAsyncStatus({
          error: {
            error: 'SQL parse failed',
            errorMessage:
              'Encountered "ALL LIMIT" at line 10, column 16.\nWas expecting one of:\n    "HOUR" ...\n    "DAY" ...\n    "MONTH" ...\n    "YEAR" ...\n    "ALL" "TIME" ...\n    "+" ...\n    "-" ...\n    "NOT" ...\n    "EXISTS" ...\n    <UNSIGNED_INTEGER_LITERAL> ...\n    <DECIMAL_NUMERIC_LITERAL> ...\n    <APPROX_NUMERIC_LITERAL> ...\n    <BINARY_STRING_LITERAL> ...\n    <PREFIXED_STRING_LITERAL> ...\n    <QUOTED_STRING> ...\n    <UNICODE_STRING_LITERAL> ...\n    "TRUE" ...\n    "FALSE" ...\n    "UNKNOWN" ...\n    "NULL" ...\n    <LBRACE_D> ...\n    <LBRACE_T> ...\n    <LBRACE_TS> ...\n    "DATE" ...\n    "TIME" ...\n    "TIMESTAMP" ...\n    "INTERVAL" ...\n    "?" ...\n    "CAST" ...\n    "EXTRACT" ...\n    "POSITION" ...\n    "CONVERT" ...\n    "TRANSLATE" ...\n    "OVERLAY" ...\n    "FLOOR" ...\n    "CEIL" ...\n    "CEILING" ...\n    "SUBSTRING" ...\n    "TRIM" ...\n    "CLASSIFIER" ...\n    "MATCH_NUMBER" ...\n    "RUNNING" ...\n    "PREV" ...\n    "NEXT" ...\n    "JSON_EXISTS" ...\n    "JSON_VALUE" ...\n    "JSON_QUERY" ...\n    "JSON_OBJECT" ...\n    "JSON_OBJECTAGG" ...\n    "JSON_ARRAY" ...\n    "JSON_ARRAYAGG" ...\n    <LBRACE_FN> ...\n    "MULTISET" ...\n    "ARRAY" ...\n    "PERIOD" ...\n    "SPECIFIC" ...\n    <IDENTIFIER> ...\n    <QUOTED_IDENTIFIER> ...\n    <BACK_QUOTED_IDENTIFIER> ...\n    <BRACKET_QUOTED_IDENTIFIER> ...\n    <UNICODE_QUOTED_IDENTIFIER> ...\n    "ABS" ...\n    "AVG" ...\n    "CARDINALITY" ...\n    "CHAR_LENGTH" ...\n    "CHARACTER_LENGTH" ...\n    "COALESCE" ...\n    "COLLECT" ...\n    "COVAR_POP" ...\n    "COVAR_SAMP" ...\n    "CUME_DIST" ...\n    "COUNT" ...\n    "CURRENT_DATE" ...\n    "CURRENT_TIME" ...\n    "CURRENT_TIMESTAMP" ...\n    "DENSE_RANK" ...\n    "ELEMENT" ...\n    "EXP" ...\n    "FIRST_VALUE" ...\n    "FUSION" ...\n    "GROUPING" ...\n    "LAG" ...\n    "LEAD" ...\n    "LEFT" ...\n    "LAST_VALUE" ...\n    "LN" ...\n    "LOCALTIME" ...\n    "LOCALTIMESTAMP" ...\n    "LOWER" ...\n    "MAX" ...\n    "MIN" ...\n    "MINUTE" ...\n    "MOD" ...\n    "NTH_VALUE" ...\n    "NTILE" ...\n    "NULLIF" ...\n    "OCTET_LENGTH" ...\n    "PERCENT_RANK" ...\n    "POWER" ...\n    "RANK" ...\n    "REGR_COUNT" ...\n    "REGR_SXX" ...\n    "REGR_SYY" ...\n    "RIGHT" ...\n    "ROW_NUMBER" ...\n    "SECOND" ...\n    "SQRT" ...\n    "STDDEV_POP" ...\n    "STDDEV_SAMP" ...\n    "SUM" ...\n    "UPPER" ...\n    "TRUNCATE" ...\n    "USER" ...\n    "VAR_POP" ...\n    "VAR_SAMP" ...\n    "CURRENT_CATALOG" ...\n    "CURRENT_DEFAULT_TRANSFORM_GROUP" ...\n    "CURRENT_PATH" ...\n    "CURRENT_ROLE" ...\n    "CURRENT_SCHEMA" ...\n    "CURRENT_USER" ...\n    "SESSION_USER" ...\n    "SYSTEM_USER" ...\n    "NEW" ...\n    "CASE" ...\n    "CURRENT" ...\n    "CURSOR" ...\n    "ROW" ...\n    "(" ...\n    ',
            errorClass: 'org.apache.calcite.sql.parser.SqlParseException',
            host: null,
          },

          asyncResultId: '8c50267b-ca15-4001-8b69-3d3b5c0db932',
          state: 'UNDETERMINED',
          engine: 'Talaria-Indexer',
        }),
      ).toMatchInlineSnapshot(`
        QueryExecution {
          "destination": undefined,
          "duration": undefined,
          "error": Object {
            "error": Object {
              "errorCode": "AsyncError",
              "errorMessage": "{\\"error\\":\\"SQL parse failed\\",\\"errorMessage\\":\\"Encountered \\\\\\"ALL LIMIT\\\\\\" at line 10, column 16.\\\\nWas expecting one of:\\\\n    \\\\\\"HOUR\\\\\\" ...\\\\n    \\\\\\"DAY\\\\\\" ...\\\\n    \\\\\\"MONTH\\\\\\" ...\\\\n    \\\\\\"YEAR\\\\\\" ...\\\\n    \\\\\\"ALL\\\\\\" \\\\\\"TIME\\\\\\" ...\\\\n    \\\\\\"+\\\\\\" ...\\\\n    \\\\\\"-\\\\\\" ...\\\\n    \\\\\\"NOT\\\\\\" ...\\\\n    \\\\\\"EXISTS\\\\\\" ...\\\\n    <UNSIGNED_INTEGER_LITERAL> ...\\\\n    <DECIMAL_NUMERIC_LITERAL> ...\\\\n    <APPROX_NUMERIC_LITERAL> ...\\\\n    <BINARY_STRING_LITERAL> ...\\\\n    <PREFIXED_STRING_LITERAL> ...\\\\n    <QUOTED_STRING> ...\\\\n    <UNICODE_STRING_LITERAL> ...\\\\n    \\\\\\"TRUE\\\\\\" ...\\\\n    \\\\\\"FALSE\\\\\\" ...\\\\n    \\\\\\"UNKNOWN\\\\\\" ...\\\\n    \\\\\\"NULL\\\\\\" ...\\\\n    <LBRACE_D> ...\\\\n    <LBRACE_T> ...\\\\n    <LBRACE_TS> ...\\\\n    \\\\\\"DATE\\\\\\" ...\\\\n    \\\\\\"TIME\\\\\\" ...\\\\n    \\\\\\"TIMESTAMP\\\\\\" ...\\\\n    \\\\\\"INTERVAL\\\\\\" ...\\\\n    \\\\\\"?\\\\\\" ...\\\\n    \\\\\\"CAST\\\\\\" ...\\\\n    \\\\\\"EXTRACT\\\\\\" ...\\\\n    \\\\\\"POSITION\\\\\\" ...\\\\n    \\\\\\"CONVERT\\\\\\" ...\\\\n    \\\\\\"TRANSLATE\\\\\\" ...\\\\n    \\\\\\"OVERLAY\\\\\\" ...\\\\n    \\\\\\"FLOOR\\\\\\" ...\\\\n    \\\\\\"CEIL\\\\\\" ...\\\\n    \\\\\\"CEILING\\\\\\" ...\\\\n    \\\\\\"SUBSTRING\\\\\\" ...\\\\n    \\\\\\"TRIM\\\\\\" ...\\\\n    \\\\\\"CLASSIFIER\\\\\\" ...\\\\n    \\\\\\"MATCH_NUMBER\\\\\\" ...\\\\n    \\\\\\"RUNNING\\\\\\" ...\\\\n    \\\\\\"PREV\\\\\\" ...\\\\n    \\\\\\"NEXT\\\\\\" ...\\\\n    \\\\\\"JSON_EXISTS\\\\\\" ...\\\\n    \\\\\\"JSON_VALUE\\\\\\" ...\\\\n    \\\\\\"JSON_QUERY\\\\\\" ...\\\\n    \\\\\\"JSON_OBJECT\\\\\\" ...\\\\n    \\\\\\"JSON_OBJECTAGG\\\\\\" ...\\\\n    \\\\\\"JSON_ARRAY\\\\\\" ...\\\\n    \\\\\\"JSON_ARRAYAGG\\\\\\" ...\\\\n    <LBRACE_FN> ...\\\\n    \\\\\\"MULTISET\\\\\\" ...\\\\n    \\\\\\"ARRAY\\\\\\" ...\\\\n    \\\\\\"PERIOD\\\\\\" ...\\\\n    \\\\\\"SPECIFIC\\\\\\" ...\\\\n    <IDENTIFIER> ...\\\\n    <QUOTED_IDENTIFIER> ...\\\\n    <BACK_QUOTED_IDENTIFIER> ...\\\\n    <BRACKET_QUOTED_IDENTIFIER> ...\\\\n    <UNICODE_QUOTED_IDENTIFIER> ...\\\\n    \\\\\\"ABS\\\\\\" ...\\\\n    \\\\\\"AVG\\\\\\" ...\\\\n    \\\\\\"CARDINALITY\\\\\\" ...\\\\n    \\\\\\"CHAR_LENGTH\\\\\\" ...\\\\n    \\\\\\"CHARACTER_LENGTH\\\\\\" ...\\\\n    \\\\\\"COALESCE\\\\\\" ...\\\\n    \\\\\\"COLLECT\\\\\\" ...\\\\n    \\\\\\"COVAR_POP\\\\\\" ...\\\\n    \\\\\\"COVAR_SAMP\\\\\\" ...\\\\n    \\\\\\"CUME_DIST\\\\\\" ...\\\\n    \\\\\\"COUNT\\\\\\" ...\\\\n    \\\\\\"CURRENT_DATE\\\\\\" ...\\\\n    \\\\\\"CURRENT_TIME\\\\\\" ...\\\\n    \\\\\\"CURRENT_TIMESTAMP\\\\\\" ...\\\\n    \\\\\\"DENSE_RANK\\\\\\" ...\\\\n    \\\\\\"ELEMENT\\\\\\" ...\\\\n    \\\\\\"EXP\\\\\\" ...\\\\n    \\\\\\"FIRST_VALUE\\\\\\" ...\\\\n    \\\\\\"FUSION\\\\\\" ...\\\\n    \\\\\\"GROUPING\\\\\\" ...\\\\n    \\\\\\"LAG\\\\\\" ...\\\\n    \\\\\\"LEAD\\\\\\" ...\\\\n    \\\\\\"LEFT\\\\\\" ...\\\\n    \\\\\\"LAST_VALUE\\\\\\" ...\\\\n    \\\\\\"LN\\\\\\" ...\\\\n    \\\\\\"LOCALTIME\\\\\\" ...\\\\n    \\\\\\"LOCALTIMESTAMP\\\\\\" ...\\\\n    \\\\\\"LOWER\\\\\\" ...\\\\n    \\\\\\"MAX\\\\\\" ...\\\\n    \\\\\\"MIN\\\\\\" ...\\\\n    \\\\\\"MINUTE\\\\\\" ...\\\\n    \\\\\\"MOD\\\\\\" ...\\\\n    \\\\\\"NTH_VALUE\\\\\\" ...\\\\n    \\\\\\"NTILE\\\\\\" ...\\\\n    \\\\\\"NULLIF\\\\\\" ...\\\\n    \\\\\\"OCTET_LENGTH\\\\\\" ...\\\\n    \\\\\\"PERCENT_RANK\\\\\\" ...\\\\n    \\\\\\"POWER\\\\\\" ...\\\\n    \\\\\\"RANK\\\\\\" ...\\\\n    \\\\\\"REGR_COUNT\\\\\\" ...\\\\n    \\\\\\"REGR_SXX\\\\\\" ...\\\\n    \\\\\\"REGR_SYY\\\\\\" ...\\\\n    \\\\\\"RIGHT\\\\\\" ...\\\\n    \\\\\\"ROW_NUMBER\\\\\\" ...\\\\n    \\\\\\"SECOND\\\\\\" ...\\\\n    \\\\\\"SQRT\\\\\\" ...\\\\n    \\\\\\"STDDEV_POP\\\\\\" ...\\\\n    \\\\\\"STDDEV_SAMP\\\\\\" ...\\\\n    \\\\\\"SUM\\\\\\" ...\\\\n    \\\\\\"UPPER\\\\\\" ...\\\\n    \\\\\\"TRUNCATE\\\\\\" ...\\\\n    \\\\\\"USER\\\\\\" ...\\\\n    \\\\\\"VAR_POP\\\\\\" ...\\\\n    \\\\\\"VAR_SAMP\\\\\\" ...\\\\n    \\\\\\"CURRENT_CATALOG\\\\\\" ...\\\\n    \\\\\\"CURRENT_DEFAULT_TRANSFORM_GROUP\\\\\\" ...\\\\n    \\\\\\"CURRENT_PATH\\\\\\" ...\\\\n    \\\\\\"CURRENT_ROLE\\\\\\" ...\\\\n    \\\\\\"CURRENT_SCHEMA\\\\\\" ...\\\\n    \\\\\\"CURRENT_USER\\\\\\" ...\\\\n    \\\\\\"SESSION_USER\\\\\\" ...\\\\n    \\\\\\"SYSTEM_USER\\\\\\" ...\\\\n    \\\\\\"NEW\\\\\\" ...\\\\n    \\\\\\"CASE\\\\\\" ...\\\\n    \\\\\\"CURRENT\\\\\\" ...\\\\n    \\\\\\"CURSOR\\\\\\" ...\\\\n    \\\\\\"ROW\\\\\\" ...\\\\n    \\\\\\"(\\\\\\" ...\\\\n    \\",\\"errorClass\\":\\"org.apache.calcite.sql.parser.SqlParseException\\",\\"host\\":null}",
            },
          },
          "id": "8c50267b-ca15-4001-8b69-3d3b5c0db932",
          "queryContext": undefined,
          "result": undefined,
          "sqlQuery": undefined,
          "stages": undefined,
          "startTime": undefined,
          "status": "FAILED",
        }
      `);
    });
  });

  describe('.fromAsyncDetail', () => {
    it('fails for bad status (error: null)', () => {
      expect(() =>
        QueryExecution.fromAsyncDetail({
          asyncResultId: 'talaria-sql-1392d806-c17f-4937-94ee-8fa0a3ce1566',
          error: null,
        } as any),
      ).toThrowError('Invalid payload');
    });

    it('fails for bad status (error: chatHandler)', () => {
      expect(() =>
        QueryExecution.fromAsyncDetail({
          error:
            "Can't find chatHandler for handler[talaria-sql-9ce6cbec-b826-48fe-80c1-4fc44f78f0d6]",
          talariaTask: {
            task: 'talaria-sql-9ce6cbec-b826-48fe-80c1-4fc44f78f0d6',
            payload: {
              type: 'talaria0',
              id: 'talaria-sql-9ce6cbec-b826-48fe-80c1-4fc44f78f0d6',
            },
          },
        } as any),
      ).toThrowError('Invalid payload');
    });

    it('works in a general case', () => {
      expect(
        QueryExecution.fromAsyncDetail({
          talaria: {
            taskId: 'talaria-sql-kttm_etl-54ba7c52-f620-48ee-93b5-90f09d3c045a',
            payload: {
              status: {
                status: 'RUNNING',
                startTime: '2022-03-05T00:46:25.079Z',
                durationMs: 2588,
              },

              stages: [
                {
                  stageNumber: 0,
                  inputStages: [],
                  stageType: 'ScanQueryFrameProcessorFactory',
                  phase: 'RESULTS_COMPLETE',
                  workerCount: 2,
                  partitionCount: 2,
                  startTime: '2022-03-05T00:46:25.488Z',
                  duration: 147,
                  clusterBy: {
                    columns: [{ columnName: '__bucket' }, { columnName: '__boost' }],
                    bucketByCount: 1,
                  },

                  query: {},
                },

                {
                  stageNumber: 1,
                  inputStages: [0],
                  stageType: 'GroupByPreShuffleFrameProcessorFactory',
                  phase: 'READING_INPUT',
                  workerCount: 2,
                  startTime: '2022-03-05T00:46:25.610Z',
                  duration: 2057,
                  clusterBy: {
                    columns: [
                      { columnName: 'd0' },
                      { columnName: 'd1' },
                      { columnName: 'd2' },
                      { columnName: 'd3' },
                      { columnName: 'd4' },
                      { columnName: 'd5' },
                      { columnName: 'd6' },
                      { columnName: 'd7' },
                      { columnName: 'd8' },
                      { columnName: 'd9' },
                      { columnName: 'd10' },
                      { columnName: 'd11' },
                      { columnName: 'd12' },
                    ],
                  },

                  query: {},
                },

                {
                  stageNumber: 2,
                  inputStages: [1],
                  stageType: 'GroupByPostShuffleFrameProcessorFactory',
                },

                {
                  stageNumber: 3,
                  inputStages: [2],
                  stageType: 'OrderByFrameProcessorFactory',
                  clusterBy: {
                    columns: [
                      { columnName: '__bucket' },
                      { columnName: 'd4' },
                      { columnName: 'd1' },
                    ],

                    bucketByCount: 1,
                  },
                },

                {
                  stageNumber: 4,
                  inputStages: [3],
                  stageType: 'TalariaSegmentGeneratorFrameProcessorFactory',
                },
              ],

              counters: [
                {
                  workerNumber: 0,
                  counters: {
                    input: [
                      { stageNumber: 1, partitionNumber: 0, frames: 1, rows: 98, bytes: 5824 },
                    ],

                    processor: [
                      { stageNumber: 0, partitionNumber: -1, frames: 1, rows: 197, bytes: 12547 },
                      { stageNumber: 1, partitionNumber: -1, frames: 0, rows: 0, bytes: 0 },
                    ],

                    output: [
                      { stageNumber: 0, partitionNumber: 0, frames: 1, rows: 98, bytes: 5824 },
                      { stageNumber: 0, partitionNumber: 1, frames: 1, rows: 99, bytes: 6003 },
                    ],
                  },

                  sortProgress: [
                    {
                      stageNumber: 0,
                      sortProgress: {
                        totalMergingLevels: 3,
                        levelToTotalBatches: { '0': 1, '1': 1, '2': 2 },
                        levelToMergedBatches: { '0': 1, '1': 1, '2': 2 },
                        totalMergersForUltimateLevel: 2,
                        progressDigest: 1.0,
                      },
                    },

                    {
                      stageNumber: 1,
                      sortProgress: {
                        totalMergingLevels: -1,
                        levelToTotalBatches: {},
                        levelToMergedBatches: {},
                        totalMergersForUltimateLevel: -1,
                      },
                    },
                  ],
                },

                {
                  workerNumber: 1,
                  counters: {
                    input: [
                      { stageNumber: 1, partitionNumber: 0, frames: 1, rows: 98, bytes: 5824 },
                      { stageNumber: 1, partitionNumber: 1, frames: 1, rows: 99, bytes: 6003 },
                    ],

                    processor: [
                      { stageNumber: 0, partitionNumber: -1, frames: 0, rows: 0, bytes: 0 },
                      { stageNumber: 1, partitionNumber: -1, frames: 0, rows: 0, bytes: 0 },
                    ],

                    output: [
                      { stageNumber: 0, partitionNumber: 0, frames: 0, rows: 0, bytes: 0 },
                      { stageNumber: 0, partitionNumber: 1, frames: 0, rows: 0, bytes: 0 },
                    ],
                  },
                  sortProgress: [
                    {
                      stageNumber: 0,
                      sortProgress: {
                        totalMergingLevels: -1,
                        levelToTotalBatches: {},
                        levelToMergedBatches: {},
                        totalMergersForUltimateLevel: -1,
                        triviallyComplete: true,
                        progressDigest: 1.0,
                      },
                    },
                    {
                      stageNumber: 1,
                      sortProgress: {
                        totalMergingLevels: -1,
                        levelToTotalBatches: {},
                        levelToMergedBatches: {},
                        totalMergersForUltimateLevel: -1,
                        triviallyComplete: true,
                        progressDigest: 1.0,
                      },
                    },
                  ],
                },
              ],
            },

            task: {
              type: 'talaria0',
              id: 'talaria-sql-kttm_etl-54ba7c52-f620-48ee-93b5-90f09d3c045a',
              spec: {
                query: {},
                columnMappings: [
                  { queryColumn: 'd0', outputColumn: '__time' },
                  { queryColumn: 'd1', outputColumn: 'session' },
                  { queryColumn: 'd2', outputColumn: 'agent_category' },
                  { queryColumn: 'd3', outputColumn: 'agent_type' },
                  { queryColumn: 'd4', outputColumn: 'browser' },
                  { queryColumn: 'd5', outputColumn: 'browser_version' },
                  { queryColumn: 'd6', outputColumn: 'browser_major' },
                  { queryColumn: 'd7', outputColumn: 'os' },
                  { queryColumn: 'd8', outputColumn: 'city' },
                  { queryColumn: 'd9', outputColumn: 'country' },
                  { queryColumn: 'd10', outputColumn: 'capital' },
                  { queryColumn: 'd11', outputColumn: 'iso3' },
                  { queryColumn: 'd12', outputColumn: 'ip_address' },
                  { queryColumn: 'a0', outputColumn: 'cnt' },
                  { queryColumn: 'a1', outputColumn: 'session_length' },
                  { queryColumn: 'a2', outputColumn: 'unique_event_types' },
                ],

                destination: {
                  type: 'dataSource',
                  dataSource: 'kttm_etl',
                  segmentGranularity: 'HOUR',
                  replaceTimeChunks: [
                    '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
                  ],
                },

                tuningConfig: {},
              },

              sqlQuery:
                '--:context talariaReplaceTimeChunks: all\n--:context talariaFinalizeAggregations: false\nINSERT INTO "kttm_etl"\nWITH\nkttm_data AS (\nSELECT * FROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]\'\n  )\n)),\ncountry_lookup AS (\nSELECT * FROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}\',\n    \'{"type":"tsv","findColumnsFromHeader":true}\',\n    \'[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]\'\n  )\n))\n\nSELECT\n  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,\n  session,\n  agent_category,\n  agent_type,\n  browser,\n  browser_version,\n  CAST(REGEXP_EXTRACT(browser_version, \'^(\\d+)\') AS BIGINT) AS browser_major,\n  os,\n  city,\n  country,\n  country_lookup.Capital AS capital,\n  country_lookup.ISO3 AS iso3,\n  forwarded_for AS ip_address,\n\n  COUNT(*) AS "cnt",\n  SUM(session_length) AS session_length,\n  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types\nFROM kttm_data\nLEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country\nWHERE os = \'iOS\'\nGROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13\nPARTITIONED BY HOUR\nCLUSTERED BY browser, session',
              sqlQueryContext: {
                talaria: true,
                sqlQueryId: '54ba7c52-f620-48ee-93b5-90f09d3c045a',
                talariaNumTasks: 2,
                talariaFinalizeAggregations: false,
                sqlInsertSegmentGranularity: '"HOUR"',
                talariaReplaceTimeChunks: 'all',
              },

              sqlTypeNames: [
                'TIMESTAMP',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'BIGINT',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'VARCHAR',
                'OTHER',
                'OTHER',
                'OTHER',
              ],

              context: { forceTimeChunkLock: true, useLineageBasedSegmentAllocation: true },
              groupId: 'talaria-sql-kttm_etl-54ba7c52-f620-48ee-93b5-90f09d3c045a',
              dataSource: 'kttm_etl',
              resource: {
                availabilityGroup: 'talaria-sql-kttm_etl-54ba7c52-f620-48ee-93b5-90f09d3c045a',
                requiredCapacity: 1,
              },
            },
          },
        }),
      ).toMatchInlineSnapshot(`
        QueryExecution {
          "destination": Object {
            "dataSource": "kttm_etl",
            "replaceTimeChunks": Array [
              "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
            ],
            "segmentGranularity": "HOUR",
            "type": "dataSource",
          },
          "duration": 2588,
          "error": undefined,
          "id": "talaria-sql-kttm_etl-54ba7c52-f620-48ee-93b5-90f09d3c045a",
          "queryContext": Object {
            "talaria": true,
            "talariaFinalizeAggregations": false,
            "talariaNumTasks": 2,
            "talariaReplaceTimeChunks": "all",
          },
          "result": undefined,
          "sqlQuery": "--:context talariaReplaceTimeChunks: all
        --:context talariaFinalizeAggregations: false
        INSERT INTO \\"kttm_etl\\"
        WITH
        kttm_data AS (
        SELECT * FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"},{\\"name\\":\\"browser\\",\\"type\\":\\"string\\"},{\\"name\\":\\"browser_version\\",\\"type\\":\\"string\\"},{\\"name\\":\\"city\\",\\"type\\":\\"string\\"},{\\"name\\":\\"continent\\",\\"type\\":\\"string\\"},{\\"name\\":\\"country\\",\\"type\\":\\"string\\"},{\\"name\\":\\"version\\",\\"type\\":\\"string\\"},{\\"name\\":\\"event_type\\",\\"type\\":\\"string\\"},{\\"name\\":\\"event_subtype\\",\\"type\\":\\"string\\"},{\\"name\\":\\"loaded_image\\",\\"type\\":\\"string\\"},{\\"name\\":\\"adblock_list\\",\\"type\\":\\"string\\"},{\\"name\\":\\"forwarded_for\\",\\"type\\":\\"string\\"},{\\"name\\":\\"language\\",\\"type\\":\\"string\\"},{\\"name\\":\\"number\\",\\"type\\":\\"long\\"},{\\"name\\":\\"os\\",\\"type\\":\\"string\\"},{\\"name\\":\\"path\\",\\"type\\":\\"string\\"},{\\"name\\":\\"platform\\",\\"type\\":\\"string\\"},{\\"name\\":\\"referrer\\",\\"type\\":\\"string\\"},{\\"name\\":\\"referrer_host\\",\\"type\\":\\"string\\"},{\\"name\\":\\"region\\",\\"type\\":\\"string\\"},{\\"name\\":\\"remote_address\\",\\"type\\":\\"string\\"},{\\"name\\":\\"screen\\",\\"type\\":\\"string\\"},{\\"name\\":\\"session\\",\\"type\\":\\"string\\"},{\\"name\\":\\"session_length\\",\\"type\\":\\"long\\"},{\\"name\\":\\"timezone\\",\\"type\\":\\"string\\"},{\\"name\\":\\"timezone_offset\\",\\"type\\":\\"long\\"},{\\"name\\":\\"window\\",\\"type\\":\\"string\\"}]'
          )
        )),
        country_lookup AS (
        SELECT * FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://static.imply.io/lookup/country.tsv\\"]}',
            '{\\"type\\":\\"tsv\\",\\"findColumnsFromHeader\\":true}',
            '[{\\"name\\":\\"Country\\",\\"type\\":\\"string\\"},{\\"name\\":\\"Capital\\",\\"type\\":\\"string\\"},{\\"name\\":\\"ISO3\\",\\"type\\":\\"string\\"},{\\"name\\":\\"ISO2\\",\\"type\\":\\"string\\"}]'
          )
        ))

        SELECT
          FLOOR(TIME_PARSE(\\"timestamp\\") TO MINUTE) AS __time,
          session,
          agent_category,
          agent_type,
          browser,
          browser_version,
          CAST(REGEXP_EXTRACT(browser_version, '^(\\\\d+)') AS BIGINT) AS browser_major,
          os,
          city,
          country,
          country_lookup.Capital AS capital,
          country_lookup.ISO3 AS iso3,
          forwarded_for AS ip_address,

          COUNT(*) AS \\"cnt\\",
          SUM(session_length) AS session_length,
          APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
        FROM kttm_data
        LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
        WHERE os = 'iOS'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
        PARTITIONED BY HOUR
        CLUSTERED BY browser, session",
          "stages": Stages {
            "counters": Array [
              Object {
                "counters": Object {
                  "input": Array [
                    Object {
                      "bytes": 5824,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 98,
                      "stageNumber": 1,
                    },
                  ],
                  "output": Array [
                    Object {
                      "bytes": 5824,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 98,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 6003,
                      "frames": 1,
                      "partitionNumber": 1,
                      "rows": 99,
                      "stageNumber": 0,
                    },
                  ],
                  "processor": Array [
                    Object {
                      "bytes": 12547,
                      "frames": 1,
                      "partitionNumber": -1,
                      "rows": 197,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": -1,
                      "rows": 0,
                      "stageNumber": 1,
                    },
                  ],
                },
                "sortProgress": Array [
                  Object {
                    "sortProgress": Object {
                      "levelToMergedBatches": Object {
                        "0": 1,
                        "1": 1,
                        "2": 2,
                      },
                      "levelToTotalBatches": Object {
                        "0": 1,
                        "1": 1,
                        "2": 2,
                      },
                      "progressDigest": 1,
                      "totalMergersForUltimateLevel": 2,
                      "totalMergingLevels": 3,
                    },
                    "stageNumber": 0,
                  },
                  Object {
                    "sortProgress": Object {
                      "levelToMergedBatches": Object {},
                      "levelToTotalBatches": Object {},
                      "totalMergersForUltimateLevel": -1,
                      "totalMergingLevels": -1,
                    },
                    "stageNumber": 1,
                  },
                ],
                "workerNumber": 0,
              },
              Object {
                "counters": Object {
                  "input": Array [
                    Object {
                      "bytes": 5824,
                      "frames": 1,
                      "partitionNumber": 0,
                      "rows": 98,
                      "stageNumber": 1,
                    },
                    Object {
                      "bytes": 6003,
                      "frames": 1,
                      "partitionNumber": 1,
                      "rows": 99,
                      "stageNumber": 1,
                    },
                  ],
                  "output": Array [
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": 0,
                      "rows": 0,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": 1,
                      "rows": 0,
                      "stageNumber": 0,
                    },
                  ],
                  "processor": Array [
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": -1,
                      "rows": 0,
                      "stageNumber": 0,
                    },
                    Object {
                      "bytes": 0,
                      "frames": 0,
                      "partitionNumber": -1,
                      "rows": 0,
                      "stageNumber": 1,
                    },
                  ],
                },
                "sortProgress": Array [
                  Object {
                    "sortProgress": Object {
                      "levelToMergedBatches": Object {},
                      "levelToTotalBatches": Object {},
                      "progressDigest": 1,
                      "totalMergersForUltimateLevel": -1,
                      "totalMergingLevels": -1,
                      "triviallyComplete": true,
                    },
                    "stageNumber": 0,
                  },
                  Object {
                    "sortProgress": Object {
                      "levelToMergedBatches": Object {},
                      "levelToTotalBatches": Object {},
                      "progressDigest": 1,
                      "totalMergersForUltimateLevel": -1,
                      "totalMergingLevels": -1,
                      "triviallyComplete": true,
                    },
                    "stageNumber": 1,
                  },
                ],
                "workerNumber": 1,
              },
            ],
            "stages": Array [
              Object {
                "clusterBy": Object {
                  "bucketByCount": 1,
                  "columns": Array [
                    Object {
                      "columnName": "__bucket",
                    },
                    Object {
                      "columnName": "__boost",
                    },
                  ],
                },
                "duration": 147,
                "inputStages": Array [],
                "partitionCount": 2,
                "phase": "RESULTS_COMPLETE",
                "query": Object {},
                "stageNumber": 0,
                "stageType": "ScanQueryFrameProcessorFactory",
                "startTime": "2022-03-05T00:46:25.488Z",
                "workerCount": 2,
              },
              Object {
                "clusterBy": Object {
                  "columns": Array [
                    Object {
                      "columnName": "d0",
                    },
                    Object {
                      "columnName": "d1",
                    },
                    Object {
                      "columnName": "d2",
                    },
                    Object {
                      "columnName": "d3",
                    },
                    Object {
                      "columnName": "d4",
                    },
                    Object {
                      "columnName": "d5",
                    },
                    Object {
                      "columnName": "d6",
                    },
                    Object {
                      "columnName": "d7",
                    },
                    Object {
                      "columnName": "d8",
                    },
                    Object {
                      "columnName": "d9",
                    },
                    Object {
                      "columnName": "d10",
                    },
                    Object {
                      "columnName": "d11",
                    },
                    Object {
                      "columnName": "d12",
                    },
                  ],
                },
                "duration": 2057,
                "inputStages": Array [
                  0,
                ],
                "phase": "READING_INPUT",
                "query": Object {},
                "stageNumber": 1,
                "stageType": "GroupByPreShuffleFrameProcessorFactory",
                "startTime": "2022-03-05T00:46:25.610Z",
                "workerCount": 2,
              },
              Object {
                "inputStages": Array [
                  1,
                ],
                "stageNumber": 2,
                "stageType": "GroupByPostShuffleFrameProcessorFactory",
              },
              Object {
                "clusterBy": Object {
                  "bucketByCount": 1,
                  "columns": Array [
                    Object {
                      "columnName": "__bucket",
                    },
                    Object {
                      "columnName": "d4",
                    },
                    Object {
                      "columnName": "d1",
                    },
                  ],
                },
                "inputStages": Array [
                  2,
                ],
                "stageNumber": 3,
                "stageType": "OrderByFrameProcessorFactory",
              },
              Object {
                "inputStages": Array [
                  3,
                ],
                "stageNumber": 4,
                "stageType": "TalariaSegmentGeneratorFrameProcessorFactory",
              },
            ],
          },
          "startTime": 2022-03-05T00:46:25.079Z,
          "status": "RUNNING",
        }
      `);
    });
  });
});
