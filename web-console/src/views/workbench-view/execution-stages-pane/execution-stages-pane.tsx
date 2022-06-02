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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Tooltip2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { ReactNode } from 'react';
import ReactTable from 'react-table';

import { BracedText } from '../../../components';
import { DEFAULT_TABLE_CLASS_NAME } from '../../../react-table';
import {
  formatBytes,
  formatDuration,
  formatDurationWithMs,
  formatInteger,
  formatPercent,
  NumberLike,
} from '../../../utils';
import {
  ClusterBy,
  compareDetailEntries,
  COUNTER_TYPE_TITLE,
  CounterType,
  DataSource,
  Execution,
  formatClusterBy,
  INPUT_COUNTERS,
  StageDefinition,
  Stages,
  summarizeInputSource,
} from '../../../workbench-models';

import { ExecutionStageDetailPane } from './execution-stage-detail-pane/execution-stage-detail-pane';

import './execution-stages-pane.scss';

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
      return String(dataSource.name);

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

function formatBreakdown(breakdown: Record<string, number>): string {
  return Object.keys(breakdown)
    .map(k => `${k}: ${formatInteger(breakdown[k])}`)
    .join('\n');
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
const formatRowRate = formatInteger;
const formatSize = (bytes: number) => `(${formatBytes(bytes)})`;
const formatByteRate = (byteRate: number) => `(${formatBytes(byteRate)}/s)`;
const formatFrames = formatInteger;
const formatDurationDynamic = (n: NumberLike) =>
  n < 1000 ? formatDurationWithMs(n) : formatDuration(n);

export interface ExecutionStagesPaneProps {
  execution: Execution;
  onErrorClick?: () => void;
  onWarningClick?: () => void;
}

export const ExecutionStagesPane = React.memo(function ExecutionStagesPane(
  props: ExecutionStagesPaneProps,
) {
  const { execution, onErrorClick, onWarningClick } = props;
  const stages = execution.stages || new Stages([]);
  const error = execution.error;

  const rowRateValues = stages.stages.map(s => formatRowRate(stages.getRowRateFromStage(s) || 0));
  const byteRateValues = stages.stages.map(s =>
    formatByteRate(stages.getByteRateFromStage(s) || 0),
  );

  const rowsValues = stages.stages.flatMap(stage => [
    formatRows(stages.getTotalInputForStage(stage, 'rows')),
    formatRows(stages.getTotalCounterForStage(stage, 'processor', 'rows')),
    formatRows(stages.getTotalCounterForStage(stage, 'sort', 'rows')),
  ]);

  const bytesAndFilesValues = stages.stages.flatMap(stage => [
    formatSize(stages.getTotalInputForStage(stage, 'bytes')),
    formatSize(stages.getTotalCounterForStage(stage, 'processor', 'bytes')),
    formatSize(stages.getTotalCounterForStage(stage, 'sort', 'bytes')),
    stage.inputFileCount
      ? `(${formatInteger(stage.inputFileCount)} GB ${formatInteger(stage.inputFileCount)})`
      : '',
  ]);

  function detailedStats(stage: StageDefinition) {
    const { phase } = stage;
    return (
      <div className="execution-stage-detail-pane-container">
        {detailedStatsForPartition(stage, 'input', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'inputExternal', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'inputDruid', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'inputStageChannel', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'processor', phase === 'READING_INPUT')}
        {detailedStatsForWorker(stage, 'sort', phase !== 'RESULTS_READY')}
        {detailedStatsForPartition(stage, 'output', phase !== 'RESULTS_READY')}
      </div>
    );
  }

  function detailedStatsForWorker(
    stage: StageDefinition,
    counterType: CounterType,
    inProgress: boolean,
  ) {
    if (!stages.hasCounterForStage(stage, counterType)) return;

    const workerEntries = stages.getByWorkerCountForStage(stage, counterType);

    workerEntries.sort(compareDetailEntries);

    return (
      <ExecutionStageDetailPane
        title={`${COUNTER_TYPE_TITLE[counterType]} counters`}
        labelPrefix="W"
        entries={workerEntries}
        inProgress={inProgress}
      />
    );
  }

  function detailedStatsForPartition(
    stage: StageDefinition,
    type: 'input' | 'output',
    inProgress: boolean,
  ) {
    const partitionEntries = stages.getByPartitionCountForStage(stage, type);
    if (!partitionEntries) return;

    partitionEntries.sort(compareDetailEntries);

    return (
      <ExecutionStageDetailPane
        title={`${capitalizeFirst(type)} partitions`}
        labelPrefix="P"
        entries={partitionEntries}
        inProgress={inProgress}
      />
    );
  }

  function dataProcessedInput(stage: StageDefinition) {
    if (!stages.hasInputCounterForStage(stage)) return;
    const inputSizeBytes = stages.getTotalInputForStage(stage, 'bytes');

    const tooltipLines: string[] = [];
    INPUT_COUNTERS.forEach(counterType => {
      if (stages.hasCounterForStage(stage, counterType)) {
        let tooltipLine = `${COUNTER_TYPE_TITLE[counterType]}: ${formatRows(
          stages.getTotalCounterForStage(stage, counterType, 'rows'),
        )} rows`;

        const bytes = stages.getTotalCounterForStage(stage, counterType, 'bytes');
        if (bytes) {
          tooltipLine += ` (${formatBytes(bytes)})`;
        }

        tooltipLines.push(tooltipLine);
      }
    });

    return (
      <div className="data-transfer" title={tooltipLines.join('\n')}>
        <div className="counter-type-label">Input:</div>
        <BracedText
          text={formatRows(stages.getTotalInputForStage(stage, 'rows'))}
          braces={rowsValues}
        />
        {stage.inputFileCount ? (
          <>
            {' '}
            &nbsp;{' '}
            <BracedText
              text={`(${formatInteger(
                stages.getTotalInputForStage(stage, 'files'),
              )} / ${formatInteger(stage.inputFileCount)})`}
              braces={bytesAndFilesValues}
            />
          </>
        ) : inputSizeBytes ? (
          <>
            {' '}
            &nbsp; <BracedText text={formatSize(inputSizeBytes)} braces={bytesAndFilesValues} />
          </>
        ) : undefined}
      </div>
    );
  }

  function dataProcessedProcessor(stage: StageDefinition) {
    if (!stages.hasCounterForStage(stage, 'processor')) return;

    return (
      <div
        className="data-transfer"
        title={`${COUNTER_TYPE_TITLE['processor']} frames: ${formatFrames(
          stages.getTotalCounterForStage(stage, 'processor', 'frames'),
        )}`}
      >
        <div className="counter-type-label">{COUNTER_TYPE_TITLE['processor'] + ':'}</div>
        <BracedText
          text={formatRows(stages.getTotalCounterForStage(stage, 'processor', 'rows'))}
          braces={rowsValues}
        />{' '}
        &nbsp;{' '}
        <BracedText
          text={formatSize(stages.getTotalCounterForStage(stage, 'processor', 'bytes'))}
          braces={bytesAndFilesValues}
        />
      </div>
    );
  }

  function dataProcessedSort(stage: StageDefinition) {
    const hasCounter = stages.hasCounterForStage(stage, 'sort');
    const hasProgress = stages.hasSortProgressForStage(stage);
    if (!hasCounter && !hasProgress) return;

    const sortRows = stages.getTotalCounterForStage(stage, 'sort', 'rows');
    const sortProgress = stages.getSortProgressForStage(stage);
    return (
      <div
        className="data-transfer"
        title={`${COUNTER_TYPE_TITLE['sort']} frames: ${formatFrames(
          stages.getTotalCounterForStage(stage, 'sort', 'frames'),
        )}`}
      >
        <div className="counter-type-label">{COUNTER_TYPE_TITLE['sort'] + ':'}</div>
        {sortRows ? (
          <>
            <BracedText text={formatRows(sortRows)} braces={rowsValues} /> &nbsp;{' '}
            <BracedText
              text={formatSize(stages.getTotalCounterForStage(stage, 'sort', 'bytes'))}
              braces={bytesAndFilesValues}
            />
            {0 < sortProgress && sortProgress < 1 && (
              <div className="sort-percent">{`[${formatPercent(sortProgress)}]`}</div>
            )}
          </>
        ) : (
          <BracedText text={`[${formatPercent(sortProgress)}]`} braces={rowsValues} />
        )}
      </div>
    );
  }

  const colorMap = stages.stagesToColorMap();

  return (
    <ReactTable
      className={classNames('execution-stages-pane', DEFAULT_TABLE_CLASS_NAME)}
      data={stages.stages}
      loading={false}
      noDataText="No stages"
      sortable={false}
      collapseOnDataChange={false}
      columns={[
        {
          Header: twoLines('Stage', <i>stage_number = Processor(inputs)</i>),
          id: 'stage',
          accessor: 'stageNumber',
          className: 'padded',
          width: 400,
          Cell(props) {
            const stage = props.original as StageDefinition;
            const dataSourceStr = formatDataSource(stage.query?.dataSource);
            const warnings = stages.getWarningCountForStage(stage);
            return (
              <div className="stage-description">
                <span
                  style={{ color: colorMap[stage.stageNumber] }}
                >{`S${stage.stageNumber}`}</span>
                {` = ${Stages.getCleanProcessorType(stage)}(`}
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
                {warnings > 0 && (
                  <>
                    {' '}
                    <Tooltip2
                      content={
                        <pre>{formatBreakdown(stages.getWarningBreakdownForStage(stage))}</pre>
                      }
                    >
                      <Button
                        minimal
                        small
                        icon={IconNames.WARNING_SIGN}
                        text={warnings > 1 ? `${warnings}` : undefined}
                        intent={Intent.WARNING}
                        onClick={onWarningClick}
                      />
                    </Tooltip2>
                  </>
                )}
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
                      <Button
                        minimal
                        small
                        icon={IconNames.ERROR}
                        intent={Intent.DANGER}
                        onClick={onErrorClick}
                      />
                    </Tooltip2>
                  </>
                )}
              </div>
            );
          },
        },
        {
          Header: twoLines('Data processed', <i>rows &nbsp; (size or files)</i>),
          id: 'data_processed',
          accessor: () => null,
          className: 'padded',
          width: 310,
          Cell({ original }) {
            return (
              <>
                {dataProcessedInput(original)}
                {dataProcessedProcessor(original)}
                {dataProcessedSort(original)}
              </>
            );
          },
        },
        {
          Header: twoLines('Data processing rate', <i>rows/s &nbsp; (data rate)</i>),
          id: 'data_processing_rate',
          accessor: s => stages.getRowRateFromStage(s),
          className: 'padded',
          width: 200,
          Cell({ original }) {
            const rowRate = stages.getRowRateFromStage(original);
            if (typeof rowRate !== 'number') return null;

            const byteRate = stages.getByteRateFromStage(original);
            return (
              <>
                <BracedText text={formatRowRate(rowRate)} braces={rowRateValues} />
                {byteRate ? (
                  <>
                    {' '}
                    &nbsp; <BracedText text={formatByteRate(byteRate)} braces={byteRateValues} />
                  </>
                ) : undefined}
              </>
            );
          },
        },
        {
          Header: 'Phase',
          id: 'phase',
          accessor: row => (row.phase ? capitalizeFirst(row.phase.replace(/_/g, ' ')) : ''),
          className: 'padded',
          width: 130,
        },
        {
          Header: 'Timing',
          id: 'timing',
          accessor: row => row.startTime,
          className: 'padded',
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
          className: 'padded',
          width: 75,
        },
        {
          Header: twoLines('Output', 'partitions'),
          accessor: 'partitionCount',
          className: 'padded',
          width: 75,
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
      defaultPageSize={10}
      showPagination={stages.stageCount() > 10}
    />
  );
});
