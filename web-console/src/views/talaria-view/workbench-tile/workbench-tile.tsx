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

import { Button, ButtonGroup, InputGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import React, { useEffect, useRef, useState } from 'react';

import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { TalariaHistory } from '../../../singletons/talaria-history';
import {
  fitExternalConfigPattern,
  LastQueryInfo,
  summarizeExternalConfig,
  TalariaQuery,
  TalariaSummary,
} from '../../../talaria-models';
import {
  ColumnMetadata,
  DruidError,
  IntermediateQueryState,
  QueryAction,
  RowColumn,
} from '../../../utils';
import { QueryContext } from '../../../utils/query-context';
import { QueryError } from '../../query-view/query-error/query-error';
import { QueryTimer } from '../../query-view/query-timer/query-timer';
import { ExportDialog } from '../export-dialog/export-dialog';
import { InsertSuccess } from '../insert-success/insert-success';
import { useMetadataStateStore } from '../metadata-state-store';
import { QueryOutput2 } from '../query-output2/query-output2';
import { RunPreviewButton } from '../run-preview-button/run-preview-button';
import { StageProgress } from '../stage-progress/stage-progress';
import { TalariaExtraInfo } from '../talaria-extra-info/talaria-extra-info';
import { TalariaQueryError } from '../talaria-query-error/talaria-query-error';
import { TalariaQueryInput } from '../talaria-query-input/talaria-query-input';
import { TalariaStats } from '../talaria-stats/talaria-stats';
import { TalariaTabCache } from '../talaria-tab-cache';
import {
  getTaskIdFromQueryResults,
  killTaskOnCancel,
  talariaBackgroundStatusCheck,
} from '../talaria-utils';
import { useWorkStateStore } from '../work-state-store';

import './workbench-tile.scss';

const queryRunner = new QueryRunner();

export interface WorkbenchTileProps {
  query: TalariaQuery;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newTalariaQuery: TalariaQuery): void;
  onDelete(): void;
  onStats(taskId: string): void;
}

export const WorkbenchTile = React.memo(function WorkbenchTile(props: WorkbenchTileProps) {
  const { query, columnMetadata, mandatoryQueryContext, onQueryChange, onDelete, onStats } = props;
  const [exportDialogQuery, setExportDialogQuery] = useState<TalariaQuery | undefined>();

  const handleExport = usePermanentCallback(() => {
    setExportDialogQuery(query);
  });

  const handleQueryStringChange = usePermanentCallback((queryString: string, _run?: boolean) => {
    onQueryChange(query.changeQueryString(queryString));
  });

  const parsedQuery = query.getParsedQuery();
  const handleQueryAction = usePermanentCallback((queryAction: QueryAction) => {
    if (!(parsedQuery instanceof SqlQuery)) return;
    onQueryChange(query.changeQueryString(parsedQuery.apply(queryAction).toString()));
  });

  const queryInputRef = useRef<TalariaQueryInput | null>(null);

  const id = query.getId();
  const [querySummaryState, queryManager] = useQueryManager<
    TalariaQuery | LastQueryInfo,
    TalariaSummary,
    TalariaSummary,
    DruidError
  >({
    initQuery: TalariaTabCache.getState(id) ? undefined : query.getLastQueryInfo(),
    initState: TalariaTabCache.getState(id),
    processQuery: async (q: TalariaQuery | LastQueryInfo, cancelToken) => {
      if (q instanceof TalariaQuery) {
        const { query, context, prefixLines } = q.getEffectiveQueryAndContext();

        const queryContext = { ...context, ...(mandatoryQueryContext || {}) };
        let result: QueryResult;
        try {
          result = await queryRunner.runQuery({
            query,
            extraQueryContext: queryContext,
            cancelToken,
          });
        } catch (e) {
          onQueryChange(props.query.changeLastQueryInfo(undefined));
          throw new DruidError(e, prefixLines);
        }

        const taskId = getTaskIdFromQueryResults(result);
        if (!taskId) {
          onQueryChange(props.query.changeLastQueryInfo(undefined));
          // return result;
          throw new Error('direct result not supported');
        }

        TalariaHistory.attachTaskId(q, taskId);
        killTaskOnCancel(taskId, cancelToken, true);
        onQueryChange(
          props.query.changeLastQueryInfo({
            taskId,
          }),
        );

        return new IntermediateQueryState(
          TalariaSummary.init(taskId, typeof query === 'string' ? query : undefined, queryContext),
        );
      } else {
        const { taskId } = q;
        killTaskOnCancel(taskId, cancelToken, true);
        return new IntermediateQueryState(TalariaSummary.reattach(taskId), 0);
      }
    },
    backgroundStatusCheck: talariaBackgroundStatusCheck,
  });

  useEffect(() => {
    if (querySummaryState.data || querySummaryState.error) {
      TalariaTabCache.storeState(id, querySummaryState);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [querySummaryState.data, querySummaryState.error]);

  const incrementWorkVersion = useWorkStateStore(state => state.increment);
  useEffect(() => {
    incrementWorkVersion();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [querySummaryState.loading]);

  const querySummary = querySummaryState.data;

  const incrementMetadataVersion = useMetadataStateStore(state => state.increment);
  useEffect(() => {
    if (querySummary?.isSuccessfulInsert()) {
      incrementMetadataVersion();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Boolean(querySummary?.isSuccessfulInsert())]);

  function moveToPosition(position: RowColumn) {
    const currentQueryInput = queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  function handleRun(actuallyIngest = false) {
    if (query.isJsonLike() && !query.validRune()) return;

    TalariaHistory.addQueryToHistory(query);
    queryManager.runQuery(actuallyIngest ? query : query.removeInsert());
  }

  const emptyQuery = query.isEmptyQuery();

  const runeMode = query.isJsonLike();
  const collapsed = query.getCollapsed();
  const insertDatasource = query.getInsertDatasource();

  const statsTaskId: string | undefined = querySummary?.taskId;

  let extraInfo: string | undefined;
  if (collapsed && parsedQuery instanceof SqlQuery) {
    try {
      extraInfo = summarizeExternalConfig(fitExternalConfigPattern(parsedQuery));
    } catch {}
  }

  return (
    <div className="workbench-tile">
      <div className="query-top-bar">
        <Button
          icon={collapsed ? IconNames.CARET_RIGHT : IconNames.CARET_DOWN}
          minimal
          small
          onClick={() => onQueryChange(query.changeCollapsed(!collapsed))}
        />
        {insertDatasource ? (
          `<insert query : ${insertDatasource}>`
        ) : (
          <>
            {collapsed ? (
              <span className="query-name">{query.getQueryName()}</span>
            ) : (
              <InputGroup
                className="query-name"
                value={query.getQueryName()}
                onChange={(e: any) => {
                  onQueryChange(query.changeQueryName(e.target.value));
                }}
                small
              />
            )}
            <span className="as-label">AS</span>
            {extraInfo && <span className="extra-info">{extraInfo}</span>}
          </>
        )}
        <ButtonGroup className="corner">
          <Popover2
            content={
              <Menu>
                <MenuItem text="Duplicate" onClick={() => onQueryChange(query.duplicateLast())} />
              </Menu>
            }
          >
            <Button icon={IconNames.MORE} minimal small />
          </Popover2>
          <Button
            icon={IconNames.CROSS}
            minimal
            small
            onClick={() => {
              TalariaTabCache.deleteState(id);
              onDelete();
            }}
          />
        </ButtonGroup>
      </div>
      {!collapsed && (
        <>
          <TalariaQueryInput
            ref={queryInputRef}
            autoHeight
            queryString={query.getQueryString()}
            onQueryStringChange={handleQueryStringChange}
            runeMode={runeMode}
            columnMetadata={
              columnMetadata ? columnMetadata.concat(query.getInlineMetadata()) : undefined
            }
          />
          <div className="query-control-bar">
            <RunPreviewButton
              query={query}
              onRun={emptyQuery ? undefined : handleRun}
              onExplain={undefined}
              onExport={handleExport}
              loading={querySummaryState.loading}
              small
            />
            {querySummaryState.intermediate && !querySummaryState.intermediate.isReattach() && (
              <QueryTimer />
            )}
            {querySummary?.result && (
              <TalariaExtraInfo
                queryResult={querySummary.result}
                onStats={() => onStats(statsTaskId!)}
              />
            )}
            {(querySummary || querySummaryState.error) && (
              <Button
                className="reset"
                icon={IconNames.CROSS}
                minimal
                small
                onClick={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastQueryInfo(undefined));
                }}
              />
            )}
          </div>
          {!querySummaryState.isInit() && (
            <div className="output-pane">
              {querySummary &&
                (querySummary.result ? (
                  <QueryOutput2
                    runeMode={runeMode}
                    queryResult={querySummary.result}
                    onExport={handleExport}
                    onQueryAction={handleQueryAction}
                    initPageSize={5}
                  />
                ) : querySummary.isSuccessfulInsert() ? (
                  <InsertSuccess
                    insertSummary={querySummary}
                    onStats={() => onStats(statsTaskId!)}
                    onQueryChange={handleQueryStringChange}
                  />
                ) : querySummary.error ? (
                  <div className="stats-container">
                    <TalariaQueryError taskError={querySummary.error} />
                    {querySummaryState && (
                      <TalariaStats stages={querySummaryState as any} error={undefined} />
                    )}
                  </div>
                ) : (
                  <div>Unknown query summary state</div>
                ))}
              {querySummaryState.error && (
                <QueryError
                  error={querySummaryState.error}
                  moveCursorTo={position => {
                    moveToPosition(position);
                  }}
                  queryString={query.getQueryString()}
                  onQueryStringChange={handleQueryStringChange}
                />
              )}
              {querySummaryState.intermediate && (
                <div className="stats-container">
                  <StageProgress
                    reattach={querySummaryState.intermediate.isReattach()}
                    stages={querySummaryState.intermediate.stages}
                    onCancel={() => queryManager.cancelCurrent()}
                  />
                </div>
              )}
            </div>
          )}
        </>
      )}
      {exportDialogQuery && (
        <ExportDialog
          talariaQuery={exportDialogQuery}
          onClose={() => {
            setExportDialogQuery(undefined);
          }}
        />
      )}
    </div>
  );
});
