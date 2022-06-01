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

import { Stages } from './stages';

describe('Stages', () => {
  describe('#stagesToColorMap', () => {
    it('works with single color', () => {
      expect(
        new Stages([
          {
            stageNumber: 0,
            inputStages: [],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 1,
            inputStages: [0],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 2,
            inputStages: [1],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
        ]).stagesToColorMap(),
      ).toEqual({
        '0': '#8dd3c7',
        '1': '#8dd3c7',
        '2': '#8dd3c7',
      });
    });

    it('works with multiple colors', () => {
      expect(
        new Stages([
          {
            stageNumber: 0,
            inputStages: [],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 1,
            inputStages: [0],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 2,
            inputStages: [],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 3,
            inputStages: [2],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 4,
            inputStages: [3, 1],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
          {
            stageNumber: 5,
            inputStages: [4],
            processorType: 'a',
            workerCount: 1,
            partitionCount: 1,
          },
        ]).stagesToColorMap(),
      ).toEqual({
        '0': '#bebada',
        '1': '#bebada',
        '2': '#ffffb3',
        '3': '#ffffb3',
        '4': '#8dd3c7',
        '5': '#8dd3c7',
      });
    });
  });

  describe('#overallProgress', () => {
    const testStages = new Stages(
      [
        {
          stageNumber: 0,
          inputStages: [],
          processorType: 'ScanQueryFrameProcessorFactory',
          phase: 'RESULTS_READY',
          workerCount: 1,
          partitionCount: 1,
          inputFileCount: 1,
          startTime: '2022-04-02T15:16:59.719Z',
          duration: 18326,
          clusterBy: { columns: [{ columnName: '__boost' }] },
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
              inputFormat: { type: 'json', flattenSpec: null, featureSpec: {} },
              signature: [
                { name: 'timestamp', type: 'STRING' },
                { name: 'agent_category', type: 'STRING' },
                { name: 'agent_type', type: 'STRING' },
                { name: 'browser', type: 'STRING' },
                { name: 'browser_version', type: 'STRING' },
                { name: 'city', type: 'STRING' },
                { name: 'continent', type: 'STRING' },
                { name: 'country', type: 'STRING' },
                { name: 'version', type: 'STRING' },
                { name: 'event_type', type: 'STRING' },
                { name: 'event_subtype', type: 'STRING' },
                { name: 'loaded_image', type: 'STRING' },
                { name: 'adblock_list', type: 'STRING' },
                { name: 'forwarded_for', type: 'STRING' },
                { name: 'language', type: 'STRING' },
                { name: 'number', type: 'STRING' },
                { name: 'os', type: 'STRING' },
                { name: 'path', type: 'STRING' },
                { name: 'platform', type: 'STRING' },
                { name: 'referrer', type: 'STRING' },
                { name: 'referrer_host', type: 'STRING' },
                { name: 'region', type: 'STRING' },
                { name: 'remote_address', type: 'STRING' },
                { name: 'screen', type: 'STRING' },
                { name: 'session', type: 'STRING' },
                { name: 'session_length', type: 'LONG' },
                { name: 'timezone', type: 'STRING' },
                { name: 'timezone_offset', type: 'STRING' },
                { name: 'window', type: 'STRING' },
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
                expression: 'mv_to_array("language")',
                outputType: 'ARRAY<STRING>',
              },
            ],
            resultFormat: 'compactedList',
            batchSize: 20480,
            filter: null,
            columns: [
              'adblock_list',
              'agent_category',
              'agent_type',
              'browser',
              'browser_version',
              'city',
              'continent',
              'country',
              'event_subtype',
              'event_type',
              'forwarded_for',
              'loaded_image',
              'number',
              'os',
              'path',
              'platform',
              'referrer',
              'referrer_host',
              'region',
              'remote_address',
              'screen',
              'session',
              'session_length',
              'timestamp',
              'timezone',
              'timezone_offset',
              'v0',
              'version',
              'window',
            ],
            legacy: false,
            context: {},
            descending: false,
            granularity: { type: 'all' },
          },
        },
        {
          stageNumber: 1,
          inputStages: [0],
          processorType: 'TalariaSegmentGeneratorFrameProcessorFactory',
          phase: 'READING_INPUT',
          workerCount: 1,
          partitionCount: 1,
          startTime: '2022-04-02T15:17:18.030Z',
          duration: 2698,
        },
      ],
      [
        {
          workerNumber: 0,
          counters: {
            inputStageChannel: [
              { stageNumber: 1, partitionNumber: 0, frames: 0, rows: 0, bytes: 0 },
            ],
            inputExternal: [
              {
                stageNumber: 0,
                partitionNumber: -1,
                frames: 0,
                rows: 465346,
                bytes: 0,
                files: 1,
              },
            ],
            processor: [
              {
                stageNumber: 0,
                partitionNumber: -1,
                frames: 48,
                rows: 465346,
                bytes: 269490599,
              },
            ],
            sort: [
              {
                stageNumber: 0,
                partitionNumber: 0,
                frames: 383,
                rows: 465346,
                bytes: 267735745,
              },
            ],
          },
          sortProgress: [
            {
              stageNumber: 0,
              sortProgress: {
                totalMergingLevels: 4,
                levelToTotalBatches: { '0': 16, '1': 6, '2': 2, '3': 1 },
                levelToMergedBatches: { '0': 16, '1': 6, '2': 2, '3': 1 },
                totalMergersForUltimateLevel: 1,
                progressDigest: 1.0,
              },
            },
          ],
          warningCounters: [
            {
              stageNumber: 0,
              warningCounters: {
                warningCount: {},
              },
            },
          ],
        },
      ],
    );

    it('works', () => {
      expect(testStages.overallProgress()).toBeCloseTo(0.5);
    });
  });
});
