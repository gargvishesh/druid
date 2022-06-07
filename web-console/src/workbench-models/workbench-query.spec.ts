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

describe('WorkbenchQuery', () => {
  beforeAll(() => {
    WorkbenchQuery.setSqlTaskEnabled(true);
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
    it('works', () => {
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
  });

  describe('#expload', () => {
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
        --:context talariaFinalizeAggregations: false
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

      expect(WorkbenchQuery.blank().changeQueryString(sql).explodeQuery().getQueryString())
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
});
