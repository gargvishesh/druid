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

import { TalariaQuery } from '../../talaria-models';

import { TabEntry } from './talaria-utils';

export function getDemoQueries(): TabEntry[] {
  return [
    {
      id: 'demo1',
      tabName: 'Demo 1',
      query: TalariaQuery.blank().changeQueryString(
        `
INSERT INTO "kttm_simple_v2"
SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)
`.trim(),
      ),
    },
    {
      id: 'demo2',
      tabName: 'Demo 2',
      query: TalariaQuery.blank().changeQueryString(
        `
INSERT INTO "kttm_rollup_v2" --:context talariaSegmentGranularity: hour

WITH kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
))

SELECT
  FLOOR(
    TIME_PARSE("timestamp") -- Timestamp parsing
    TO MINUTE -- Query granularity
  ) AS __time,
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
  screen,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_BUILTIN_RAW(event_type) AS unique_event_types
FROM kttm_data
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
ORDER BY session -- Secondary partitioning
`.trim(),
      ),
    },
    {
      id: 'demo3',
      tabName: 'Demo 3',
      query: TalariaQuery.blank().changeQueryString(
        `
INSERT INTO "kttm_etl_v2" --:context talariaSegmentGranularity: hour
WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)),
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))

SELECT
  FLOOR(
    TIME_PARSE("timestamp") -- Timestamp parsing
    TO MINUTE -- Query granularity
  ) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  city,
  continent,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO2 AS iso2,
  country_lookup.ISO3 AS iso3,
  region,
  adblock_list,
  forwarded_for,
  os,
  path,
  platform,
  referrer,
  referrer_host,
  remote_address,
  screen,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_BUILTIN_RAW(event_type) AS unique_event_types
FROM kttm_data
LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
ORDER BY session -- Secondary partitioning
`.trim(),
      ),
    },
    {
      id: 'demo4a',
      tabName: 'Demo 4a',
      query: TalariaQuery.blank().changeQueryString(
        `
INSERT INTO "kttm_reingest_v2" --:context talariaSegmentGranularity: hour
WITH
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))

SELECT
  FLOOR(
    TIME_PARSE("timestamp") -- Timestamp parsing
    TO MINUTE -- Query granularity
  ) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  city,
  continent,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO2 AS iso2,
  country_lookup.ISO3 AS iso3,
  region,
  adblock_list,
  forwarded_for,
  os,
  path,
  platform,
  referrer,
  referrer_host,
  remote_address,
  screen,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_BUILTIN_RAW(event_type) AS unique_event_types
FROM kttm_simple_v2
LEFT JOIN country_lookup ON country_lookup.Country = kttm_simple_v2.country
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
ORDER BY session -- Secondary partitioning
`.trim(),
      ),
    },
    {
      id: 'demo4b',
      tabName: 'Demo 4b',
      query: TalariaQuery.blank().changeQueryString(
        `
INSERT INTO kttm_simple_v2 --:context talariaReplaceTimeChunks: all
SELECT *
FROM kttm_simple_v2
WHERE NOT(country = 'New Zealand')
`.trim(),
      ),
    },
    {
      id: 'demo5',
      tabName: 'Demo 5',
      query: TalariaQuery.blank().changeQueryString(
        `
WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)),
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))
SELECT
  os,
  CONCAT(country, ' (', country_lookup.ISO3, ')') AS "country",
  COUNT(DISTINCT session) AS "unique_sessions"
FROM kttm_data
LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10
`.trim(),
      ),
    },
    {
      id: 'demo6',
      tabName: 'Demo 6',
      query: TalariaQuery.blank()
        .changeQueryString(
          `
SELECT *
FROM "kttm_simple_v2"
ORDER BY
  session ASC,
  number ASC
`.trim(),
        )
        .changeQueryContext({ talaria: true }),
    },
  ];
}
