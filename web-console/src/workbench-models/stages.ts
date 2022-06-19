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

import { sum } from 'd3-array';
import hasOwnProp from 'has-own-prop';

import { InputFormat } from '../druid-models/input-format';
import { InputSource } from '../druid-models/input-source';
import { filterMap, oneOf } from '../utils';

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

const SORT_WEIGHT = 0.5;
const READING_INPUT_WITH_SORT_WEIGHT = 1 - SORT_WEIGHT;

function zeroDivide(a: number, b: number): number {
  if (b === 0) return 0;
  return a / b;
}

export interface StageDefinition {
  stageNumber: number;
  inputStages: number[];
  inputFileCount?: number;
  processorType: string;
  query?: {
    queryType: string;
    dataSource: DataSource;
    [k: string]: any;
  };
  startTime?: string;
  duration?: number;
  phase?: 'NEW' | 'READING_INPUT' | 'POST_READING' | 'RESULTS_READY' | 'FINISHED' | 'FAILED';
  workerCount: number;
  partitionCount: number;
  clusterBy?: ClusterBy;
}

export interface DataSource {
  type: 'external' | 'stage' | 'table' | 'join';
  name?: string;
  inputSource: InputSource;
  inputFormat: InputFormat;
  left?: DataSource;
  right?: DataSource;
  signature?: { name: string; type: string }[];
}

export interface ClusterBy {
  columns: {
    columnName: string;
    descending?: boolean;
  }[];
  bucketByCount?: number;
}

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

export type CounterType =
  | 'inputExternal'
  | 'inputDruid'
  | 'inputStageChannel'
  | 'processor'
  | 'sort';

export const INPUT_COUNTERS: CounterType[] = ['inputExternal', 'inputDruid', 'inputStageChannel'];

export const COUNTER_TYPE_TITLE: Record<CounterType, string> = {
  inputExternal: 'Input external',
  inputDruid: 'Input druid',
  inputStageChannel: 'Input stage',
  processor: 'Processor output',
  sort: 'Sort output',
};

export interface WorkerCounter {
  workerNumber: number;
  counters: Partial<Record<CounterType, StageCounter[]>>;
  sortProgress: {
    stageNumber: number;
    sortProgress: SortProgress;
  }[];
  warningCounters: {
    stageNumber: number;
    warningCounters: { warningCount: Record<string, number> };
  }[];
}

function tallyWarningCount(warningCount: Record<string, number>): number {
  return sum(Object.values(warningCount));
}

function sumByKey(objs: Record<string, number>[]): Record<string, number> {
  const res: Record<string, number> = {};
  for (const obj of objs) {
    for (const k in obj) {
      if (hasOwnProp(obj, k)) {
        res[k] = (res[k] || 0) + obj[k];
      }
    }
  }
  return res;
}

export interface StageCounter {
  stageNumber: number;
  partitionNumber: number;
  rows: number;
  bytes: number;
  frames: number;
  files?: number; // only present on inputExternal
}

export interface SimpleCounter {
  index: number;
  rows: number;
  bytes: number;
  frames: number;
}

export interface SortProgress {
  totalMergingLevels: number;
  levelToTotalBatches: Record<number, number>;
  levelToMergedBatches: Record<number, number>;
  totalMergersForUltimateLevel: number;
  progressDigest?: number;
  triviallyComplete?: boolean;
}

export function compareDetailEntries(a: SimpleCounter, b: SimpleCounter): number {
  const diff = b.rows - a.rows;
  if (diff) return diff;
  return a.index - b.index;
}

export class Stages {
  static readonly QUERY_START_FACTOR = 0.05;
  static readonly QUERY_END_FACTOR = 0.05;

  static getCleanProcessorType(stage: StageDefinition): string {
    return String(stage.processorType)
      .replace(/^MSQ/, '')
      .replace(/FrameProcessorFactory$/, '');
  }

  static stageWeight(stage: StageDefinition): number {
    return stage.processorType === 'OffsetLimitFrameProcessorFactory' ? 0.1 : 1;
  }

  static stageHasSort(stage: StageDefinition): boolean {
    return (stage.clusterBy?.columns?.length || 0) > 0;
  }

  static stageOutputCounterType(stage: StageDefinition): CounterType {
    return Stages.stageHasSort(stage) ? 'sort' : 'processor';
  }

  public readonly stages: StageDefinition[];
  private readonly counters?: WorkerCounter[];

  constructor(stages: StageDefinition[], counters?: WorkerCounter[]) {
    this.stages = stages;
    this.counters = counters;
  }

  stageCount(): number {
    return this.stages.length;
  }

  getStage(stageNumber: number): StageDefinition {
    return this.stages[stageNumber];
  }

  getLastStage(): StageDefinition | undefined {
    return this.stages[this.stages.length - 1];
  }

  overallProgress(): number {
    const { stages } = this;
    let progress = 0;
    let total = Stages.QUERY_END_FACTOR;
    if (stages.length) {
      progress +=
        Stages.QUERY_START_FACTOR + sum(stages, s => this.stageProgress(s) * Stages.stageWeight(s));
      total += Stages.QUERY_START_FACTOR + sum(stages, Stages.stageWeight);
    }
    return zeroDivide(progress, total);
  }

  stageProgress(stage: StageDefinition): number {
    switch (stage.phase) {
      case 'READING_INPUT':
        return (
          (Stages.stageHasSort(stage) ? READING_INPUT_WITH_SORT_WEIGHT : 1) *
          this.readingInputPhaseProgress(stage)
        );

      case 'POST_READING':
        return READING_INPUT_WITH_SORT_WEIGHT + SORT_WEIGHT * this.postReadingPhaseProgress(stage);

      case 'RESULTS_READY':
      case 'FINISHED':
        return 1;

      default:
        return 0;
    }
  }

  readingInputPhaseProgress(stage: StageDefinition): number {
    const { stages } = this;

    if (stage.inputFileCount) {
      // If we know how many files there are base the progress on how many files were read
      return this.getTotalCounterForStage(stage, 'inputExternal', 'files') / stage.inputFileCount;
    } else {
      // Otherwise, base it on the stage input divided by the output of all input stages
      const expectedInputStageRows = sum(stage.inputStages, i =>
        this.getTotalOutputForStage(stages[i], 'rows'),
      );

      return zeroDivide(
        this.getTotalCounterForStage(stage, 'inputStageChannel', 'rows'),
        expectedInputStageRows,
      );
    }
  }

  postReadingPhaseProgress(stage: StageDefinition): number {
    return this.getSortProgressForStage(stage);
  }

  currentStageIndex(): number {
    return this.stages.findIndex(({ phase }) => oneOf(phase, 'READING_INPUT', 'POST_READING'));
  }

  hasCounterForStage(stage: StageDefinition, counterType: CounterType): boolean {
    const { counters } = this;
    if (!counters) return false;
    const { stageNumber } = stage;
    return counters.some(w =>
      Boolean(w.counters[counterType]?.some(c => c.stageNumber === stageNumber)),
    );
  }

  hasInputCounterForStage(stage: StageDefinition): boolean {
    return (
      this.hasCounterForStage(stage, 'inputExternal') ||
      this.hasCounterForStage(stage, 'inputDruid') ||
      this.hasCounterForStage(stage, 'inputStageChannel')
    );
  }

  hasSortProgressForStage(stage: StageDefinition): boolean {
    const { counters } = this;
    if (!counters) return false;
    const { stageNumber, phase } = stage;
    if (phase === 'READING_INPUT') return false;

    return counters.some(w =>
      Boolean(
        w.sortProgress.some(
          s =>
            s.stageNumber === stageNumber &&
            s.sortProgress.progressDigest &&
            !s.sortProgress.triviallyComplete,
        ),
      ),
    );
  }

  getWarningCount(): number {
    const { counters } = this;
    if (!counters) return 0;
    return sum(counters, w =>
      sum(w.warningCounters || [], o => tallyWarningCount(o.warningCounters.warningCount)),
    );
  }

  getWarningCountForStage(stage: StageDefinition): number {
    const { counters } = this;
    if (!counters) return 0;
    const { stageNumber } = stage;
    return sum(counters, w =>
      sum(w.warningCounters || [], o =>
        o.stageNumber === stageNumber ? tallyWarningCount(o.warningCounters.warningCount) : 0,
      ),
    );
  }

  getWarningBreakdownForStage(stage: StageDefinition): Record<string, number> {
    const { counters } = this;
    if (!counters) return {};
    const { stageNumber } = stage;
    return sumByKey(
      counters.flatMap(w =>
        filterMap(w.warningCounters || [], o =>
          o.stageNumber === stageNumber ? o.warningCounters.warningCount : undefined,
        ),
      ),
    );
  }

  getTotalCounterForStage(
    stage: StageDefinition,
    counterType: CounterType,
    field: 'frames' | 'rows' | 'bytes' | 'files',
  ): number {
    const { counters } = this;
    if (!counters) return 0;
    const { stageNumber } = stage;
    return sum(counters, w =>
      sum(w.counters[counterType] || [], o => (o.stageNumber === stageNumber ? o[field] : 0)),
    );
  }

  getTotalInputForStage(
    stage: StageDefinition,
    field: 'frames' | 'rows' | 'bytes' | 'files',
  ): number {
    return (
      this.getTotalCounterForStage(stage, 'inputExternal', field) +
      this.getTotalCounterForStage(stage, 'inputDruid', field) +
      this.getTotalCounterForStage(stage, 'inputStageChannel', field)
    );
  }

  getTotalOutputForStage(stage: StageDefinition, field: 'frames' | 'rows' | 'bytes'): number {
    return this.getTotalCounterForStage(stage, Stages.stageOutputCounterType(stage), field);
  }

  getSortProgressForStage(stage: StageDefinition): number {
    const { counters } = this;
    if (!counters) return 0;
    const { stageNumber } = stage;
    return zeroDivide(
      sum(counters, w => {
        const sortInputRows = sum(w.counters.processor || [], o =>
          o.stageNumber === stageNumber ? o.rows : 0,
        );
        const progressDigest =
          w.sortProgress.find(sp => sp.stageNumber === stageNumber)?.sortProgress?.progressDigest ||
          0;
        return Math.floor(sortInputRows * progressDigest);
      }),
      this.getTotalCounterForStage(stage, 'processor', 'rows'),
    );
  }

  getByWorkerCountForStage(stage: StageDefinition, counterType: CounterType): SimpleCounter[] {
    const { counters } = this;
    const { workerCount, stageNumber } = stage;

    const simpleCounters = [];
    for (let i = 0; i < workerCount; i++) {
      let simpleCounter: SimpleCounter;

      const specificCounters = counters?.[i]?.counters?.[counterType];
      if (specificCounters) {
        simpleCounter = {
          index: i,
          rows: sum(specificCounters, c => (c.stageNumber === stageNumber ? c.rows : 0)),
          bytes: sum(specificCounters, c => (c.stageNumber === stageNumber ? c.bytes : 0)),
          frames: sum(specificCounters, c => (c.stageNumber === stageNumber ? c.frames : 0)),
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

  getByPartitionCountForStage(
    stage: StageDefinition,
    type: 'input' | 'output',
  ): SimpleCounter[] | undefined {
    const { counters } = this;
    const { stageNumber } = stage;
    const counterType: CounterType =
      type === 'input' ? 'inputStageChannel' : Stages.stageOutputCounterType(stage);

    if (!this.hasCounterForStage(stage, counterType)) return;

    const simpleCounters: SimpleCounter[] = [];
    if (type === 'output') {
      // If we are in output mode then we clearly know how many partitions to expect to initialize their counters with 0s
      for (let i = 0; i < stage.partitionCount; i++) {
        simpleCounters.push({
          index: i,
          rows: 0,
          bytes: 0,
          frames: 0,
        });
      }
    }

    if (counters) {
      for (const counter of counters) {
        const specificCounters = counter.counters[counterType];
        if (specificCounters) {
          for (const c of specificCounters) {
            if (c.stageNumber === stageNumber) {
              let mySimpleCounter = simpleCounters[c.partitionNumber];
              if (!mySimpleCounter) {
                simpleCounters[c.partitionNumber] = mySimpleCounter = {
                  index: c.partitionNumber,
                  rows: c.rows,
                  bytes: c.bytes,
                  frames: c.frames,
                };
              } else {
                mySimpleCounter.rows += c.rows;
                mySimpleCounter.bytes += c.bytes;
                mySimpleCounter.frames += c.frames;
              }
            }
          }
        }
      }
    }

    return simpleCounters;
  }

  getRowRateFromStage(stage: StageDefinition): number | undefined {
    if (!stage.duration) return;
    return Math.round(this.getTotalInputForStage(stage, 'rows') / (stage.duration / 1000));
  }

  getByteRateFromStage(stage: StageDefinition): number | undefined {
    if (
      !stage.duration ||
      this.hasCounterForStage(stage, 'inputExternal') ||
      this.hasCounterForStage(stage, 'inputDruid')
    ) {
      return; // If re have inputs that do not report bytes, don't show a rate
    }
    return (
      this.getTotalCounterForStage(stage, 'inputStageChannel', 'bytes') / (stage.duration / 1000)
    );
  }

  stagesToColorMap(): Record<number, string> {
    let paletteIndex = 0;
    const colorMap: Record<number, string> = {};
    for (let i = this.stages.length - 1; i >= 0; i--) {
      const { stageNumber, inputStages } = this.stages[i];
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
}
