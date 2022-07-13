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

import { WorkbenchQuery } from './workbench-query';
import { WorkbenchQueryPart } from './workbench-query-part';

describe('WorkbenchQuery', () => {
  beforeAll(() => {
    WorkbenchQuery.setQueryEngines(['native', 'sql', 'sql-task']);
  });

  describe('.commentOutInsertInto', () => {
    it('works when INSERT INTO is first', () => {
      const sql = sane`
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when REPLACE INTO is first', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --PLACE INTO trips2 OVERWRITE ALL
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is not first', () => {
      const sql = sane`
        --:context some_key: some_value
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --:context some_key: some_value
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is indented', () => {
      const sql = sane`
        --:context some_key: some_value
            INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --:context some_key: some_value
            --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });
  });

  describe('#getInsertDatasource', () => {
    it('works with INSERT', () => {
      const sql = sane`
        --:context some_key: some_value
        INSERT INTO trips2
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
            EXTERN(
              '{"type": "local", ...}',
              '{"type":"csv", ...}',
              '[{ "name": "cab_type", "type": "string" }, ...]'
            )
          )
        CLUSTERED BY trip_id
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql').getIngestDatasource()).toBeUndefined();
    });

    it('works with INSERT (unparsable)', () => {
      const sql = sane`
        --:context some_key: some_value
        INSERT INTO trips2
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql').getIngestDatasource()).toBeUndefined();
    });

    it('works with REPLACE', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
            EXTERN(
              '{"type": "local", ...}',
              '{"type":"csv", ...}',
              '[{ "name": "cab_type", "type": "string" }, ...]'
            )
          )
        CLUSTERED BY trip_id
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql').getIngestDatasource()).toBeUndefined();
    });

    it('works with REPLACE (unparsable)', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        WITH kttm_data AS (SELECT *
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql').getIngestDatasource()).toBeUndefined();
    });
  });

  describe('#extractCteHelpers', () => {
    it('works', () => {
      const sql = sane`
        REPLACE INTO task_statuses OVERWRITE ALL
        WITH
        task_statuses AS (
        SELECT * FROM
        TABLE(
          EXTERN(
            '{"type":"local","baseDir":"/Users/vadim/Desktop/","filter":"task_statuses.json"}',
            '{"type":"json"}',
            '[{"name":"id","type":"string"},{"name":"status","type":"string"},{"name":"duration","type":"long"},{"name":"errorMsg","type":"string"},{"name":"created_date","type":"string"}]'
          )
        )
        )
        (
        --:context msqFinalizeAggregations: false
        --:context groupByEnableMultiValueUnnesting: false
        --PLACE INTO task_statuses OVERWRITE ALL
        SELECT
          id,
          status,
          duration,
          errorMsg,
          created_date
        FROM task_statuses
        --RTITIONED BY ALL
        )
        PARTITIONED BY ALL
      `;

      expect(WorkbenchQuery.blank().changeQueryString(sql).extractCteHelpers().getQueryString())
        .toEqual(sane`
          REPLACE INTO task_statuses OVERWRITE ALL
          SELECT
            id,
            status,
            duration,
            errorMsg,
            created_date
          FROM task_statuses
          PARTITIONED BY ALL
        `);
    });
  });

  describe('#materializeHelpers', () => {
    expect(
      WorkbenchQuery.blank()
        .changeQueryParts([
          new WorkbenchQueryPart({
            id: 'aaa',
            queryName: 'kttm_data',
            queryString: sane`
            SELECT * FROM TABLE(
              EXTERN(
                '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
                '{"type":"json"}',
                '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
              )
            )
        `,
          }),
          new WorkbenchQueryPart({
            id: 'bbb',
            queryName: 'country_lookup',
            queryString: sane`
            SELECT * FROM TABLE(
              EXTERN(
                '{"type":"http","uris":["https://static.imply.io/lookup/country.tsv"]}',
                '{"type":"tsv","findColumnsFromHeader":true}',
                '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
              )
            )
        `,
          }),
          new WorkbenchQueryPart({
            id: 'ccc',
            queryName: 'x',
            queryString: sane`
            SELECT
              os,
              CONCAT(country, ' (', country_lookup.ISO3, ')') AS "country",
              COUNT(DISTINCT session) AS "unique_sessions"
            FROM kttm_data
            LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 10
        `,
          }),
        ])
        .materializeHelpers(),
    );
  });
});
