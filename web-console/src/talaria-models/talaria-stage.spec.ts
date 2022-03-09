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

import { Stages } from './talaria-stage';

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
});
