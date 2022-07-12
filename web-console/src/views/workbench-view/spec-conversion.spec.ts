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

import { sane } from 'druid-query-toolkit';

import { convertSpecToSql } from './spec-conversion';

describe('spec conversion', () => {
  it('converts spec (without rollup)', () => {
    expect(
      convertSpecToSql({
        type: 'index_parallel',
        spec: {
          ioConfig: {
            type: 'index_parallel',
            inputSource: {
              type: 'http',
              uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
            },
            inputFormat: {
              type: 'json',
            },
          },
          dataSchema: {
            granularitySpec: {
              segmentGranularity: 'hour',
              queryGranularity: 'none',
              rollup: false,
            },
            dataSource: 'wikipedia',
            timestampSpec: {
              column: 'timestamp',
              format: 'auto',
            },
            dimensionsSpec: {
              dimensions: [
                'isRobot',
                'channel',
                'flags',
                'isUnpatrolled',
                'page',
                'diffUrl',
                {
                  type: 'long',
                  name: 'added',
                },
                'comment',
                {
                  type: 'long',
                  name: 'commentLength',
                },
                'isNew',
                'isMinor',
                {
                  type: 'long',
                  name: 'delta',
                },
                'isAnonymous',
                'user',
                {
                  type: 'long',
                  name: 'deltaBucket',
                },
                {
                  type: 'long',
                  name: 'deleted',
                },
                'namespace',
                'cityName',
                'countryName',
                'regionIsoCode',
                'metroCode',
                'countryIsoCode',
                'regionName',
              ],
            },
          },
          tuningConfig: {
            type: 'index_parallel',
            partitionsSpec: {
              type: 'single_dim',
              partitionDimension: 'isRobot',
              targetRowsPerSegment: 150000,
            },
            forceGuaranteedRollup: true,
            maxNumConcurrentSubTasks: 4,
            maxParseExceptions: 3,
          },
        },
      } as any),
    ).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      --:context msqMaxNumTasks: 5
      --:context maxParseExceptions: 3
      --:context msqFinalizeAggregations: false
      --:context groupByEnableMultiValueUnnesting: false
      REPLACE INTO wikipedia OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"page","type":"string"},{"name":"diffUrl","type":"string"},{"name":"added","type":"long"},{"name":"comment","type":"string"},{"name":"commentLength","type":"long"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"delta","type":"long"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"}]'
        )
      ))
      SELECT
        CASE WHEN CAST("timestamp" AS BIGINT) > 0 THEN MILLIS_TO_TIMESTAMP(CAST("timestamp" AS BIGINT)) ELSE TIME_PARSE("timestamp") END AS __time,
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "page",
        "diffUrl",
        "added",
        "comment",
        "commentLength",
        "isNew",
        "isMinor",
        "delta",
        "isAnonymous",
        "user",
        "deltaBucket",
        "deleted",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName"
      FROM source
      PARTITIONED BY HOUR
      CLUSTERED BY "isRobot"
    `);
  });

  it('converts spec (with rollup)', () => {
    expect(
      convertSpecToSql({
        type: 'index_parallel',
        spec: {
          ioConfig: {
            type: 'index_parallel',
            inputSource: {
              type: 'http',
              uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
            },
            inputFormat: {
              type: 'json',
            },
          },
          dataSchema: {
            granularitySpec: {
              segmentGranularity: 'hour',
              queryGranularity: 'hour',
            },
            dataSource: 'wikipedia_rollup',
            timestampSpec: {
              column: 'timestamp',
              format: 'iso',
            },
            dimensionsSpec: {
              dimensions: [
                'isRobot',
                'channel',
                'flags',
                'isUnpatrolled',
                'comment',
                'isNew',
                'isMinor',
                'isAnonymous',
                'user',
                'namespace',
                'cityName',
                'countryName',
                'regionIsoCode',
                'metroCode',
                'countryIsoCode',
                'regionName',
              ],
            },
            metricsSpec: [
              {
                name: 'count',
                type: 'count',
              },
              {
                name: 'sum_added',
                type: 'longSum',
                fieldName: 'added',
              },
              {
                name: 'sum_commentLength',
                type: 'longSum',
                fieldName: 'commentLength',
              },
              {
                name: 'sum_delta',
                type: 'longSum',
                fieldName: 'delta',
              },
              {
                name: 'sum_deltaBucket',
                type: 'longSum',
                fieldName: 'deltaBucket',
              },
              {
                name: 'sum_deleted',
                type: 'longSum',
                fieldName: 'deleted',
              },
              {
                name: 'page_theta',
                type: 'thetaSketch',
                fieldName: 'page',
              },
            ],
          },
          tuningConfig: {
            type: 'index_parallel',
            partitionsSpec: {
              type: 'hashed',
            },
            forceGuaranteedRollup: true,
          },
        },
      } as any),
    ).toEqual(sane`
      -- This SQL query was auto generated from an ingestion spec
      --:context msqFinalizeAggregations: false
      --:context groupByEnableMultiValueUnnesting: false
      REPLACE INTO wikipedia_rollup OVERWRITE ALL
      WITH source AS (SELECT * FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"isRobot","type":"string"},{"name":"channel","type":"string"},{"name":"flags","type":"string"},{"name":"isUnpatrolled","type":"string"},{"name":"comment","type":"string"},{"name":"isNew","type":"string"},{"name":"isMinor","type":"string"},{"name":"isAnonymous","type":"string"},{"name":"user","type":"string"},{"name":"namespace","type":"string"},{"name":"cityName","type":"string"},{"name":"countryName","type":"string"},{"name":"regionIsoCode","type":"string"},{"name":"metroCode","type":"string"},{"name":"countryIsoCode","type":"string"},{"name":"regionName","type":"string"},{"name":"added","type":"long"},{"name":"commentLength","type":"long"},{"name":"delta","type":"long"},{"name":"deltaBucket","type":"long"},{"name":"deleted","type":"long"},{"name":"page","type":"string"}]'
        )
      ))
      SELECT
        TIME_FLOOR(TIME_PARSE("timestamp"), 'PT1H') AS __time,
        "isRobot",
        "channel",
        "flags",
        "isUnpatrolled",
        "comment",
        "isNew",
        "isMinor",
        "isAnonymous",
        "user",
        "namespace",
        "cityName",
        "countryName",
        "regionIsoCode",
        "metroCode",
        "countryIsoCode",
        "regionName",
        COUNT(*) AS "count",
        SUM("added") AS "sum_added",
        SUM("commentLength") AS "sum_commentLength",
        SUM("delta") AS "sum_delta",
        SUM("deltaBucket") AS "sum_deltaBucket",
        SUM("deleted") AS "sum_deleted",
        APPROX_COUNT_DISTINCT_DS_THETA("page") AS "page_theta"
      FROM source
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
      PARTITIONED BY HOUR
    `);
  });
});
