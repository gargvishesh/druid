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

import { max, sum } from 'd3-array';

import { InputFormat } from '../druid-models/input-format';
import { InputSource } from '../druid-models/input-source';
import { formatBytes, formatInteger, oneOf } from '../utils';

export interface StageDefinition {
  stageNumber: number;
  inputStages: number[];
  stageType: string;
  query?: {
    dataSource: DataSource;
  };
  startTime?: string;
  duration?: number;
  phase?: 'NEW' | 'READING_INPUT' | 'POST_READING' | 'RESULTS_COMPLETE' | 'FAILED';
  workerCount: number;
  clusterBy?: ClusterBy;
  workerCounters: WorkerCounter[];
}

export interface DataSource {
  type: 'external' | 'stage' | 'table' | 'join';
  name: string;
  inputSource: InputSource;
  inputFormat: InputFormat;
  left: DataSource;
  right: DataSource;
}

export interface ClusterBy {
  columns: {
    columnName: string;
    descending: boolean;
  }[];
  bucketByCount?: number;
}

export interface WorkerCounter {
  workerNumber: number;
  counters: {
    input?: StageCounter[];
    preShuffle?: StageCounter[];
    output?: StageCounter[];
  };
}

export interface StageCounter {
  stageNumber: number;
  partitionNumber: number;
  frames: number;
  rows: number;
  bytes: number;
}

export interface SimpleCounter {
  index: number;
  rows: number;
  bytes: number;
  frames: number;
}

export function compareDetailEntries(a: SimpleCounter, b: SimpleCounter): number {
  const diff = b.rows - a.rows;
  if (diff) return diff;
  return a.index - b.index;
}

const QUERY_START_FACTOR = 0.1;
const QUERY_END_FACTOR = 0.1;

const CLEAN_STAGE_TYPE_META_INFO: Record<string, { weight: number; hasPostReading: boolean }> = {
  ScanQuery: { weight: 0.9, hasPostReading: true },
  GroupByPreShuffle: { weight: 1.5, hasPostReading: true },
  GroupByPostShuffle: { weight: 1.5, hasPostReading: true },
  OrderBy: { weight: 1, hasPostReading: true },
  Limit: { weight: 0.1, hasPostReading: true },
  TalariaSegmentGenerator: { weight: 1.1, hasPostReading: false },
};

export function cleanStageType(stageType: string): string {
  return stageType.replace(/FrameProcessorFactory$/, '');
}

function stageWeight(stage: StageDefinition): number {
  return CLEAN_STAGE_TYPE_META_INFO[cleanStageType(stage.stageType)]?.weight || 1;
}

function stageHasPostReading(stage: StageDefinition): boolean {
  return CLEAN_STAGE_TYPE_META_INFO[cleanStageType(stage.stageType)]?.hasPostReading ?? true;
}

export function getStartTime(stages: StageDefinition[] | undefined): Date | undefined {
  if (!Array.isArray(stages)) return;
  const startTime = stages[0].startTime;
  if (typeof startTime !== 'string') return;
  const date = new Date(startTime);
  if (isNaN(date.valueOf())) return;
  return date;
}

export function overallProgress(stages: StageDefinition[] | undefined): number {
  let progress = 0;
  let total = QUERY_END_FACTOR;
  if (stages && stages.length) {
    progress += QUERY_START_FACTOR + sum(stages, s => stageProgress(s, stages) * stageWeight(s));
    total += QUERY_START_FACTOR + sum(stages, stageWeight);
  }
  return progress / total;
}

export function currentStageIndex(stages: StageDefinition[] | undefined): number {
  if (stages) {
    for (let i = 0; i < stages.length; i++) {
      if (oneOf(stages[i].phase, 'READING_INPUT', 'POST_READING')) return i;
    }
  }
  return -1;
}

function zeno(steps: number): number {
  return 1 - Math.pow(0.5, steps);
}

export function stageProgress(stage: StageDefinition, stages: StageDefinition[]): number {
  switch (stage.phase) {
    case 'READING_INPUT': {
      const currentInputRows =
        getTotalCountForStage(stage, 'input', 'rows') ||
        getTotalCountForStage(stage, 'preShuffle', 'rows') ||
        0;

      const expectedInputRows = sum(stage.inputStages, i =>
        getTotalCountForStage(stages[i], 'output', 'rows'),
      );

      const phaseProgress = expectedInputRows
        ? currentInputRows / expectedInputRows
        : zeno(currentInputRows / 1000000);

      if (stageHasPostReading(stage)) {
        return 0.45 * phaseProgress;
      } else {
        return 0.9 * phaseProgress;
      }
    }

    case 'POST_READING': {
      const phaseProgress =
        getTotalCountForStage(stage, 'output', 'rows') /
        (getTotalCountForStage(stage, 'preShuffle', 'rows') ||
          getTotalCountForStage(stage, 'input', 'rows'));

      return 0.55 + 0.35 * phaseProgress;
    }

    case 'RESULTS_COMPLETE':
      return 1;

    default:
      return 0;
  }
}

export type CounterType = 'input' | 'preShuffle' | 'output';

export const COUNTER_TYPES: CounterType[] = ['input', 'preShuffle', 'output'];

export const COUNTER_TYPE_TITLE: Record<CounterType, string> = {
  input: 'Input',
  preShuffle: 'Pre shuffle',
  output: 'Output',
};

export function formatClusterBy(
  clusterBy: ClusterBy | undefined,
  part: 'all' | 'partition' | 'cluster' = 'all',
): string {
  if (!clusterBy) return '';

  let { columns, bucketByCount } = clusterBy;
  if (bucketByCount) {
    if (part === 'partition') {
      columns = columns.slice(0, bucketByCount);
    } else if (bucketByCount) {
      columns = columns.slice(bucketByCount);
    }
  }

  return columns.map(part => part.columnName + (part.descending ? ' DESC' : '')).join(', ');
}

export function hasCounterTypeForStage(stage: StageDefinition, counterType: CounterType): boolean {
  return stage.workerCounters.some(w => Boolean(w.counters[counterType]));
}

export function getNumPartitions(
  stage: StageDefinition,
  counterType: CounterType,
): number | undefined {
  const { workerCounters } = stage;
  if (!workerCounters.length) return;
  const m = max(workerCounters, w => max(w.counters[counterType] || [], o => o.partitionNumber));
  if (typeof m === 'undefined') return;
  return m + 1;
}

export function getTotalCountForStage(
  stage: StageDefinition,
  counterType: CounterType,
  field: 'frames' | 'rows' | 'bytes',
): number {
  return sum(stage.workerCounters, w => sum(w.counters[counterType] || [], o => o[field]));
}

export function getByWorkerCountForStage(
  stage: StageDefinition,
  counterType: CounterType,
): SimpleCounter[] {
  const numWorkers = stage.workerCount;

  const simpleCounters = [];
  for (let i = 0; i < numWorkers; i++) {
    let simpleCounter: SimpleCounter;

    const counters = stage.workerCounters[i]?.counters?.[counterType];
    if (counters) {
      simpleCounter = {
        index: i,
        rows: sum(counters, counter => counter.rows),
        bytes: sum(counters, counter => counter.bytes),
        frames: sum(counters, counter => counter.frames),
      };
    } else {
      simpleCounter = {
        index: i,
        rows: 0,
        bytes: 0,
        frames: 0,
      };
    }

    simpleCounters.push(simpleCounter);
  }

  return simpleCounters;
}

export function getByPartitionCountForStage(
  stage: StageDefinition,
  counterType: CounterType,
): SimpleCounter[] | undefined {
  const numPartitions = getNumPartitions(stage, counterType);
  if (!numPartitions) return;

  const simpleCounters = [];
  for (let i = 0; i < numPartitions; i++) {
    simpleCounters.push({
      index: i,
      rows: 0,
      bytes: 0,
      frames: 0,
    });
  }

  for (const w of stage.workerCounters) {
    const counters = w.counters[counterType];
    if (counters) {
      for (const o of counters) {
        const mySimpleCounter = simpleCounters[o.partitionNumber];
        mySimpleCounter.rows += o.rows;
        mySimpleCounter.bytes += o.bytes;
        mySimpleCounter.frames += o.frames;
      }
    }
  }

  return simpleCounters;
}

export function getRateCounterType(stage: StageDefinition): CounterType | undefined {
  const { workerCounters } = stage;
  const workerCounter = workerCounters[0];
  if (!workerCounter) return;
  if (workerCounter.counters.input) return 'input';
  if (workerCounter.counters.preShuffle) return 'preShuffle';
  if (workerCounter.counters.output) return 'output';
  return;
}

export function getRowRateFromStage(stage: StageDefinition): string {
  const rateCounterType = getRateCounterType(stage);
  if (!rateCounterType || !stage.duration) return '';
  return formatInteger(
    Math.round((getTotalCountForStage(stage, rateCounterType, 'rows') * 1000) / stage.duration),
  );
}

export function getByteRateFromStage(stage: StageDefinition): string {
  const rateCounterType = getRateCounterType(stage);
  if (!rateCounterType || !stage.duration) return '';
  return `(${formatBytes(
    (getTotalCountForStage(stage, rateCounterType, 'bytes') * 1000) / stage.duration,
  )}/s)`;
}

const PALETTE = [
  '#8dd3c7',
  '#ffffb3',
  '#bebada',
  '#fb8072',
  '#80b1d3',
  '#fdb462',
  '#b3de69',
  '#fccde5',
  '#d9d9d9',
  '#bc80bd',
  '#ccebc5',
  '#ffed6f',
];

export function stagesToColorMap(stages: StageDefinition[]): Record<number, string> {
  let paletteIndex = 0;
  const colorMap: Record<number, string> = {};
  for (let i = stages.length - 1; i >= 0; i--) {
    const { stageNumber, inputStages } = stages[i];
    if (!colorMap[stageNumber]) {
      colorMap[stageNumber] = PALETTE[paletteIndex];
      paletteIndex = (paletteIndex + 1) % PALETTE.length;
    }
    if (inputStages.length > 1) {
      inputStages.forEach(inputStage => {
        colorMap[inputStage] = PALETTE[paletteIndex];
        paletteIndex = (paletteIndex + 1) % PALETTE.length;
      });
    } else {
      inputStages.forEach(inputStage => {
        colorMap[inputStage] = colorMap[stageNumber];
      });
    }
  }
  return colorMap;
}
