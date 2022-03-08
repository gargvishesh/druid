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

import { Icon, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Tooltip2 } from '@blueprintjs/popover2';
import React, { ReactNode } from 'react';
import ReactTable from 'react-table';

import { BracedText } from '../../../components';
import {
  cleanStageType,
  ClusterBy,
  compareDetailEntries,
  COUNTER_TYPE_TITLE,
  COUNTER_TYPES,
  CounterType,
  DataSource,
  formatClusterBy,
  getByPartitionCountForStage,
  getByteRateFromStage,
  getByWorkerCountForStage,
  getNumPartitions,
  getRowRateFromStage,
  getRowSortProgressForStage,
  getTotalCountForStage,
  hasCounterTypeForStage,
  hasSortProgressForStage,
  StageDefinition,
  stagesToColorMap,
  summarizeInputSource,
  TalariaTaskError,
} from '../../../talaria-models';
import {
  formatBytes,
  formatDuration,
  formatDurationWithMs,
  formatInteger,
  NumberLike,
} from '../../../utils';

import { TalariaDetailedStats } from './talaria-detailed-stats/talaria-detailed-stats';

import './talaria-stats.scss';

function twoLines(line1: string | JSX.Element, line2: string | JSX.Element) {
  return (
    <>
      {line1}
      <br />
      {line2}
    </>
  );
}

function capitalizeFirst(str: string): string {
  return str.slice(0, 1).toUpperCase() + str.slice(1).toLowerCase();
}

function formatDataSource(dataSource: DataSource | undefined): string {
  if (!dataSource) return '';

  switch (dataSource.type) {
    case 'table':
      return dataSource.name;

    case 'external':
      return summarizeInputSource(dataSource.inputSource);

    case 'stage':
      return '';

    case 'join':
      return formatDataSource(dataSource.left) || formatDataSource(dataSource.right);

    default:
      return dataSource.type;
  }
}

function joinElements(elements: ReactNode[], separator = ', '): ReactNode[] {
  return elements.map((element, i, a) => (
    <React.Fragment key={i}>
      {element}
      {i < a.length - 1 && separator}
    </React.Fragment>
  ));
}

const formatRows = formatInteger;
const formatSize = (bytes: number) => `(${formatBytes(bytes)})`;
const formatFrames = formatInteger;
const formatDurationDynamic = (n: NumberLike) =>
  n < 1000 ? formatDurationWithMs(n) : formatDuration(n);

export interface TalariaStatsProps {
  stages: StageDefinition[];
  error?: TalariaTaskError;
}

export const TalariaStats = React.memo(function TalariaStats(props: TalariaStatsProps) {
  const { stages, error } = props;

  const rowRateValues = stages.map(getRowRateFromStage);
  const byteRateValues = stages.map(getByteRateFromStage);

  const rowsValues = COUNTER_TYPES.flatMap(counterType =>
    stages.map(stage => formatRows(getTotalCountForStage(stage, counterType, 'rows'))),
  );
  const bytesValues = COUNTER_TYPES.flatMap(counterType =>
    stages.map(stage => formatSize(getTotalCountForStage(stage, counterType, 'bytes'))),
  );

  function detailedStats(stage: StageDefinition) {
    const { phase } = stage;
    return (
      <div className="talaria-detailed-stats-container">
        {detailedStatsForWorker(stage, 'inputExternal', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'inputDruid', phase === 'READING_INPUT')}
        {detailedStatsForPartition(stage, 'input', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'input', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'preShuffle', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'output', phase !== 'RESULTS_COMPLETE')}
        {detailedStatsForPartition(stage, 'output', phase !== 'RESULTS_COMPLETE')}
      </div>
    );
  }

  function detailedStatsForWorker(
    stage: StageDefinition,
    counterType: CounterType,
    inProgress: boolean,
  ) {
    if (!hasCounterTypeForStage(stage, counterType)) return;

    const workerEntries = getByWorkerCountForStage(stage, counterType);

    workerEntries.sort(compareDetailEntries);

    return (
      <TalariaDetailedStats
        title={`${COUNTER_TYPE_TITLE[counterType]} by worker`}
        labelPrefix="W"
        entries={workerEntries}
        inProgress={inProgress}
      />
    );
  }

  function detailedStatsForPartition(
    stage: StageDefinition,
    counterType: CounterType,
    inProgress: boolean,
  ) {
    if (!hasCounterTypeForStage(stage, counterType)) return;

    const partitionEntries = getByPartitionCountForStage(stage, counterType);
    if (!partitionEntries) return;

    partitionEntries.sort(compareDetailEntries);

    return (
      <TalariaDetailedStats
        title={`${COUNTER_TYPE_TITLE[counterType]} by partition`}
        labelPrefix="P"
        entries={partitionEntries}
        inProgress={inProgress}
      />
    );
  }

  function dataTransfer(stage: StageDefinition, counterType: CounterType) {
    if (!hasCounterTypeForStage(stage, counterType)) return;

    const bytes = getTotalCountForStage(stage, counterType, 'bytes');
    return (
      <div
        className="data-transfer"
        title={`${COUNTER_TYPE_TITLE[counterType]} frames: ${formatFrames(
          getTotalCountForStage(stage, counterType, 'frames'),
        )}`}
      >
        <div className="counter-type-label">{COUNTER_TYPE_TITLE[counterType] + ':'}</div>
        <BracedText
          text={formatRows(getTotalCountForStage(stage, counterType, 'rows'))}
          braces={rowsValues}
        />{' '}
        &nbsp; {bytes > 0 && <BracedText text={formatSize(bytes)} braces={bytesValues} />}
      </div>
    );
  }

  function sortProgress(stage: StageDefinition) {
    if (!hasSortProgressForStage(stage)) return;

    return (
      <div className="data-transfer" title="Sort...">
        <div className="counter-type-label">Sort:</div>
        <BracedText text={formatRows(getRowSortProgressForStage(stage))} braces={rowsValues} />
      </div>
    );
  }

  const colorMap = stagesToColorMap(stages);

  return (
    <ReactTable
      className="talaria-stats -striped -highlight"
      data={stages}
      loading={false}
      noDataText="No stages"
      sortable={false}
      collapseOnDataChange={false}
      columns={[
        {
          Header: twoLines('Stage', <i>stage_number = StageType(inputs)</i>),
          id: 'stage',
          accessor: 'stageNumber',
          width: 400,
          Cell(props) {
            const stage = props.original as StageDefinition;
            const dataSourceStr = formatDataSource(stage.query?.dataSource);
            return (
              <div className="stage-description">
                <span
                  style={{ color: colorMap[stage.stageNumber] }}
                >{`S${stage.stageNumber}`}</span>
                {` = ${cleanStageType(stage.stageType)}(`}
                {joinElements(
                  (dataSourceStr
                    ? [
                        <span key="datasource" className="datasource">
                          {dataSourceStr}
                        </span>,
                      ]
                    : []
                  ).concat(
                    stage.inputStages.map(n => (
                      <span key={n} style={{ color: colorMap[n] }}>{`S${n}`}</span>
                    )),
                  ),
                )}
                )
                {error && error.stageNumber === stage.stageNumber && (
                  <>
                    {' '}
                    <Tooltip2
                      content={
                        <div>
                          {(error.error.errorCode ? `${error.error.errorCode}: ` : '') +
                            error.error.errorMessage}
                        </div>
                      }
                    >
                      <Icon icon={IconNames.ERROR} intent={Intent.DANGER} />
                    </Tooltip2>
                  </>
                )}
              </div>
            );
          },
        },
        {
          Header: twoLines('Transfer rate', <i>rows/s &nbsp; (data rate)</i>),
          id: 'transfer_rate',
          accessor: getRowRateFromStage,
          width: 200,
          Cell({ original }) {
            return (
              <>
                <BracedText text={getRowRateFromStage(original)} braces={rowRateValues} /> &nbsp;{' '}
                <BracedText text={getByteRateFromStage(original)} braces={byteRateValues} />
              </>
            );
          },
        },
        {
          Header: twoLines('Data transferred', <i>rows &nbsp; (size)</i>),
          id: 'data_transferred',
          accessor: row => getTotalCountForStage(row, 'input', 'rows'),
          width: 290,
          Cell({ original }) {
            return (
              <>
                {dataTransfer(original, 'inputExternal')}
                {dataTransfer(original, 'inputDruid')}
                {dataTransfer(original, 'input')}
                {dataTransfer(original, 'preShuffle')}
                {sortProgress(original)}
                {dataTransfer(original, 'output')}
              </>
            );
          },
        },
        {
          Header: 'Phase',
          id: 'phase',
          accessor: row => (row.phase ? capitalizeFirst(row.phase.replace(/_/g, ' ')) : ''),
          width: 130,
        },
        {
          Header: 'Timing',
          id: 'timing',
          accessor: row => row.startTime,
          width: 170,
          Cell({ value, original }) {
            if (!value) return null;
            return (
              <div
                title={
                  value + (original.duration ? `/${formatDurationWithMs(original.duration)}` : '')
                }
              >
                {value.replace('T', ' ').replace(/\.\d\d\dZ$/, '')}
                <br />
                {original.duration ? formatDurationDynamic(original.duration) : ''}
              </div>
            );
          },
        },
        {
          Header: twoLines('Num', 'workers'),
          accessor: 'workerCount',
          width: 75,
        },
        {
          Header: twoLines('Output', 'partitions'),
          id: 'output_partitions',
          accessor: row => getNumPartitions(row, 'output'),
          width: 75,
        },
        {
          Header: twoLines('Input files', <i>read / total</i>),
          id: 'input_files',
          width: 200,
          Cell({ original }) {
            if (!original.totalFiles) return null;
            return (
              <>
                {getTotalCountForStage(original, 'input', 'files') +
                  getTotalCountForStage(original, 'inputExternal', 'files') +
                  getTotalCountForStage(original, 'inputDruid', 'files')}{' '}
                / {original.totalFiles}
              </>
            );
          },
        },
        {
          Header: 'Cluster by',
          id: 'clusterBy',
          accessor: row => formatClusterBy(row.clusterBy),
          Cell({ value, original }) {
            const clusterBy: ClusterBy | undefined = original.clusterBy;
            if (!clusterBy) return null;
            if (clusterBy.bucketByCount) {
              return (
                <div>
                  <div>{`Partition by: ${formatClusterBy(clusterBy, 'partition')}`}</div>
                  <div>{`Cluster by: ${formatClusterBy(clusterBy, 'cluster')}`}</div>
                </div>
              );
            } else {
              return <div title={value}>{value}</div>;
            }
          },
        },
      ]}
      SubComponent={({ original }) => detailedStats(original)}
      defaultPageSize={20}
      showPagination={stages.length > 20}
    />
  );
});
