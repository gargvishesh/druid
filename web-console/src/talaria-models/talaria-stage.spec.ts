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

import { stagesToColorMap } from './talaria-stage';

describe('talaria-stage', () => {
  describe('stagesToColorMap', () => {
    it('works with single color', () => {
      expect(
        stagesToColorMap([
          { stageNumber: 0, inputStages: [], stageType: 'a', workerCount: 1, workerCounters: [] },
          { stageNumber: 1, inputStages: [0], stageType: 'a', workerCount: 1, workerCounters: [] },
          { stageNumber: 2, inputStages: [1], stageType: 'a', workerCount: 1, workerCounters: [] },
        ]),
      ).toEqual({
        '0': '#8dd3c7',
        '1': '#8dd3c7',
        '2': '#8dd3c7',
      });
    });

    it('works with multiple colors', () => {
      expect(
        stagesToColorMap([
          { stageNumber: 0, inputStages: [], stageType: 'a', workerCount: 1, workerCounters: [] },
          { stageNumber: 1, inputStages: [0], stageType: 'a', workerCount: 1, workerCounters: [] },
          { stageNumber: 2, inputStages: [], stageType: 'a', workerCount: 1, workerCounters: [] },
          { stageNumber: 3, inputStages: [2], stageType: 'a', workerCount: 1, workerCounters: [] },
          {
            stageNumber: 4,
            inputStages: [3, 1],
            stageType: 'a',
            workerCount: 1,
            workerCounters: [],
          },
          { stageNumber: 5, inputStages: [4], stageType: 'a', workerCount: 1, workerCounters: [] },
        ]),
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
});
