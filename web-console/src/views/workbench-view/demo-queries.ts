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

import { TabEntry, WorkbenchQuery } from '../../workbench-models';

const BASE_QUERY = WorkbenchQuery.blank().changeQueryContext({ talariaNumTasks: 2 });

export function getDemoQueries(): TabEntry[] {
  return [
    {
      id: 'demo1',
      tabName: 'Demo 1',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
REPLACE INTO "kttm_simple" OVERWRITE ALL
SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)
PARTITIONED BY ALL TIME
`.trim(),
      ),
    },
    {
      id: 'demo2',
      tabName: 'Demo 2',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false
REPLACE INTO "kttm_rollup" OVERWRITE ALL

WITH kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_data
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`.trim(),
      ),
    },
    {
      id: 'demo3',
      tabName: 'Demo 3',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false
REPLACE INTO "kttm_etl" OVERWRITE ALL
WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
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
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO3 AS iso3,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_data
LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`.trim(),
      ),
    },
    {
      id: 'demo4a',
      tabName: 'Demo 4a',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
--:context talariaFinalizeAggregations: false
--:context groupByEnableMultiValueUnnesting: false
REPLACE INTO "kttm_reingest" OVERWRITE ALL
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
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO3 AS iso3,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_simple
LEFT JOIN country_lookup ON country_lookup.Country = kttm_simple.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`.trim(),
      ),
    },
    {
      id: 'demo4b',
      tabName: 'Demo 4b',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
REPLACE INTO kttm_simple OVERWRITE ALL
SELECT *
FROM kttm_simple
WHERE NOT(country = 'New Zealand')
PARTITIONED BY ALL TIME
`.trim(),
      ),
    },
    {
      id: 'demo5',
      tabName: 'Demo 5',
      query: BASE_QUERY.duplicate().changeQueryString(
        `
WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
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
      query: BASE_QUERY.duplicate()
        .changeQueryString(
          `
SELECT
  session,
  number,
  browser,
  browser_version,
  "language",
  event_type,
  event_subtype
FROM "kttm_simple"
ORDER BY
  session ASC,
  number ASC
`.trim(),
        )
        .changeEngine('sql-task'),
    },
  ];
}
