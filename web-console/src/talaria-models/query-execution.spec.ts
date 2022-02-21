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
        }),
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
        }),
      ).toThrowError('Invalid payload');
    });

    it('works in a general case', () => {
      expect(
        QueryExecution.fromAsyncDetail({
          talariaStatus: {
            taskId: 'talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60',
            payload: {
              status: 'FAILED',
              startTime: '2022-02-05T18:17:16.289Z',
              durationMs: 3835,
              errorReport: {
                taskId: 'talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60',
                host: 'localhost:8091',
                error: {
                  errorCode: 'InsertCannotBeEmpty',
                  dataSource: 'kttm_part1',
                  errorMessage: 'No rows to insert for dataSource [kttm_part1]',
                },

                exceptionStackTrace:
                  'io.imply.druid.talaria.indexing.error.TalariaException: InsertCannotBeEmpty: No rows to insert for dataSource [kttm_part1]\n\tat io.imply.druid.talaria.exec.LeaderImpl.runQueryUntilDone(LeaderImpl.java:474)\n\tat io.imply.druid.talaria.exec.LeaderImpl.runTask(LeaderImpl.java:276)\n\tat io.imply.druid.talaria.exec.LeaderImpl.run(LeaderImpl.java:230)\n\tat io.imply.druid.talaria.indexing.TalariaControllerTask.run(TalariaControllerTask.java:156)\n\tat org.apache.druid.indexing.overlord.ThreadingTaskRunner$1.call(ThreadingTaskRunner.java:210)\n\tat org.apache.druid.indexing.overlord.ThreadingTaskRunner$1.call(ThreadingTaskRunner.java:152)\n\tat java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)\n\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)\n\tat java.base/java.lang.Thread.run(Thread.java:829)\n',
              },
            },
          },

          talariaTask: {
            task: 'talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60',
            payload: {
              type: 'talaria0',
              id: 'talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60',
              spec: {
                query: {
                  queryType: 'dont-care',
                },

                columnMappings: ['dont-care'],
                destination: {
                  type: 'dataSource',
                  dataSource: 'kttm_part1',
                  segmentGranularity: 'DAY',
                  replaceTimeChunks: [
                    '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
                  ],
                },

                tuningConfig: {
                  type: 'dont-care',
                },
              },

              sqlQuery:
                '--:context talariaReplaceTimeChunks: all\n--:context talariaRowsPerSegment: 40000\nINSERT INTO "kttm_part1"\n\nWITH kttm_data AS (\nSELECT * FROM TABLE(\n  EXTERN(\n    \'{"type":"http","uris":["https://file.com/file"]}\',\n    \'{"type":"json"}\',\n    \'[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]\'\n  )\n))\n\nSELECT\n  TIME_PARSE("timestamp") AS __time,\n  session,\n  agent_category,\n  agent_type,\n  browser,\n  browser_version,\n  city,\n  continent,\n  country,\n  region,\n  adblock_list,\n  forwarded_for,\n  os,\n  path,\n  platform,\n  referrer,\n  referrer_host,\n  remote_address,\n  screen\nFROM kttm_data\nWHERE session = \'woop\'\nORDER BY browser, browser_version -- Secondary partitioning',
              sqlQueryContext: {
                talaria: true,
                talariaRowsPerSegment: 40000,
                sqlQueryId: 'cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60',
                talariaNumTasks: 2,
                talariaReplaceTimeChunks: 'all',
              },

              sqlTypeNames: ['TIMESTAMP'],

              dataSource: 'kttm_part1',
            },
          },
        }),
      ).toMatchInlineSnapshot(`
        QueryExecution {
          "destination": Object {
            "dataSource": "kttm_part1",
            "replaceTimeChunks": Array [
              "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z",
            ],
            "segmentGranularity": "DAY",
            "type": "dataSource",
          },
          "duration": 3835,
          "error": Object {
            "error": Object {
              "dataSource": "kttm_part1",
              "errorCode": "InsertCannotBeEmpty",
              "errorMessage": "No rows to insert for dataSource [kttm_part1]",
            },
            "exceptionStackTrace": "io.imply.druid.talaria.indexing.error.TalariaException: InsertCannotBeEmpty: No rows to insert for dataSource [kttm_part1]
        	at io.imply.druid.talaria.exec.LeaderImpl.runQueryUntilDone(LeaderImpl.java:474)
        	at io.imply.druid.talaria.exec.LeaderImpl.runTask(LeaderImpl.java:276)
        	at io.imply.druid.talaria.exec.LeaderImpl.run(LeaderImpl.java:230)
        	at io.imply.druid.talaria.indexing.TalariaControllerTask.run(TalariaControllerTask.java:156)
        	at org.apache.druid.indexing.overlord.ThreadingTaskRunner$1.call(ThreadingTaskRunner.java:210)
        	at org.apache.druid.indexing.overlord.ThreadingTaskRunner$1.call(ThreadingTaskRunner.java:152)
        	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        	at java.base/java.lang.Thread.run(Thread.java:829)
        ",
            "host": "localhost:8091",
            "taskId": "talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60",
          },
          "id": "talaria-sql-kttm_part1-cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60",
          "queryContext": Object {
            "sqlQueryId": "cf1515b5-ec2a-4c25-8a3a-d7e2a2531a60",
            "talaria": true,
            "talariaNumTasks": 2,
            "talariaReplaceTimeChunks": "all",
            "talariaRowsPerSegment": 40000,
          },
          "result": undefined,
          "sqlQuery": "--:context talariaReplaceTimeChunks: all
        --:context talariaRowsPerSegment: 40000
        INSERT INTO \\"kttm_part1\\"

        WITH kttm_data AS (
        SELECT * FROM TABLE(
          EXTERN(
            '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://file.com/file\\"]}',
            '{\\"type\\":\\"json\\"}',
            '[{\\"name\\":\\"timestamp\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_category\\",\\"type\\":\\"string\\"},{\\"name\\":\\"agent_type\\",\\"type\\":\\"string\\"},{\\"name\\":\\"browser\\",\\"type\\":\\"string\\"},{\\"name\\":\\"browser_version\\",\\"type\\":\\"string\\"},{\\"name\\":\\"city\\",\\"type\\":\\"string\\"},{\\"name\\":\\"continent\\",\\"type\\":\\"string\\"},{\\"name\\":\\"country\\",\\"type\\":\\"string\\"},{\\"name\\":\\"version\\",\\"type\\":\\"string\\"},{\\"name\\":\\"event_type\\",\\"type\\":\\"string\\"},{\\"name\\":\\"event_subtype\\",\\"type\\":\\"string\\"},{\\"name\\":\\"loaded_image\\",\\"type\\":\\"string\\"},{\\"name\\":\\"adblock_list\\",\\"type\\":\\"string\\"},{\\"name\\":\\"forwarded_for\\",\\"type\\":\\"string\\"},{\\"name\\":\\"language\\",\\"type\\":\\"string\\"},{\\"name\\":\\"number\\",\\"type\\":\\"long\\"},{\\"name\\":\\"os\\",\\"type\\":\\"string\\"},{\\"name\\":\\"path\\",\\"type\\":\\"string\\"},{\\"name\\":\\"platform\\",\\"type\\":\\"string\\"},{\\"name\\":\\"referrer\\",\\"type\\":\\"string\\"},{\\"name\\":\\"referrer_host\\",\\"type\\":\\"string\\"},{\\"name\\":\\"region\\",\\"type\\":\\"string\\"},{\\"name\\":\\"remote_address\\",\\"type\\":\\"string\\"},{\\"name\\":\\"screen\\",\\"type\\":\\"string\\"},{\\"name\\":\\"session\\",\\"type\\":\\"string\\"},{\\"name\\":\\"session_length\\",\\"type\\":\\"long\\"},{\\"name\\":\\"timezone\\",\\"type\\":\\"string\\"},{\\"name\\":\\"timezone_offset\\",\\"type\\":\\"long\\"},{\\"name\\":\\"window\\",\\"type\\":\\"string\\"}]'
          )
        ))

        SELECT
          TIME_PARSE(\\"timestamp\\") AS __time,
          session,
          agent_category,
          agent_type,
          browser,
          browser_version,
          city,
          continent,
          country,
          region,
          adblock_list,
          forwarded_for,
          os,
          path,
          platform,
          referrer,
          referrer_host,
          remote_address,
          screen
        FROM kttm_data
        WHERE session = 'woop'
        ORDER BY browser, browser_version -- Secondary partitioning",
          "stages": undefined,
          "startTime": 2022-02-05T18:17:16.289Z,
          "status": "FAILED",
        }
      `);
    });
  });
});
