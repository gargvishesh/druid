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

import { TalariaQuery } from './talaria-query';

describe('TalariaQuery', () => {
  describe('.commentOutInsertInto', () => {
    it('works when INSERT INTO is first', () => {
      const sql = sane`
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(TalariaQuery.commentOutInsertInto(sql)).toEqual(sane`
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is not first', () => {
      const sql = sane`
        --:context talariaSegmentGranularity: month
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(TalariaQuery.commentOutInsertInto(sql)).toEqual(sane`
        --:context talariaSegmentGranularity: month
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is indented', () => {
      const sql = sane`
        --:context talariaSegmentGranularity: month
            INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(TalariaQuery.commentOutInsertInto(sql)).toEqual(sane`
        --:context talariaSegmentGranularity: month
            --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });
  });

  describe('#getInsertDatasource', () => {
    it('works', () => {
      const sql = sane`
        --:context talariaSegmentGranularity: month
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
        ORDER BY trip_id
      `;

      expect(TalariaQuery.blank().changeQueryString(sql).getInsertDatasource()).toEqual('trips2');
    });
  });
});
