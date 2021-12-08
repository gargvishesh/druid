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

import { Button, ButtonGroup, Code, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import SplitterLayout from 'react-splitter-layout';

import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { TalariaHistory } from '../../../singletons/talaria-history';
import { LastQueryInfo, TalariaQuery, TalariaSummary } from '../../../talaria-models';
import {
  ColumnMetadata,
  DruidError,
  IntermediateQueryState,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
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
import { WorkbenchTile } from '../workbench-tile/workbench-tile';

import './query-tab.scss';

const queryRunner = new QueryRunner();

export interface QueryTabProps {
  query: TalariaQuery;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newTalariaQuery: TalariaQuery): void;
  onStats(taskId: string): void;
}

export const QueryTab = React.memo(function QueryTab(props: QueryTabProps) {
  const { query, columnMetadata, mandatoryQueryContext, onQueryChange, onStats } = props;
  const [showLiveReports, setShowLiveReports] = useState(true);
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

  const handleSecondaryPaneSizeChange = useCallback((secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  }, []);

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

  const statsTaskId: string | undefined = querySummary?.taskId;

  const queryPrefixes = query.getPrefixQueries();

  return (
    <div className="query-tab">
      <SplitterLayout
        vertical
        percentage
        secondaryInitialSize={
          Number(localStorageGet(LocalStorageKeys.TALARIA_TAB_PANE_SIZE)!) || 40
        }
        primaryMinSize={30}
        secondaryMinSize={30}
        onSecondaryPaneSizeChange={handleSecondaryPaneSizeChange}
      >
        <div className="query-section">
          {queryPrefixes.map((queryPrefix, i) => (
            <WorkbenchTile
              key={queryPrefix.getId()}
              query={queryPrefix}
              mandatoryQueryContext={mandatoryQueryContext}
              columnMetadata={columnMetadata}
              onQueryChange={newQuery => {
                onQueryChange(query.applyUpdate(newQuery, i));
              }}
              onDelete={() => {
                onQueryChange(query.remove(i));
              }}
              onStats={onStats}
            />
          ))}
          <div className={classNames('main-query', queryPrefixes.length ? 'multi' : 'single')}>
            <div className="main-query-top-bar">
              <Button
                icon={IconNames.ARROW_UP}
                text="Use as WITH query"
                minimal
                small
                onClick={() => {
                  onQueryChange(query.addBlank());
                }}
              />
              <ButtonGroup className="corner">
                <Popover2
                  content={
                    <Menu>
                      <MenuItem
                        text="Duplicate"
                        onClick={() => onQueryChange(query.duplicateLast())}
                      />
                    </Menu>
                  }
                >
                  <Button icon={IconNames.MORE} minimal small />
                </Popover2>
              </ButtonGroup>
            </div>
            <TalariaQueryInput
              ref={queryInputRef}
              autoHeight
              minRows={10}
              queryString={query.getQueryString()}
              onQueryStringChange={handleQueryStringChange}
              runeMode={runeMode}
              columnMetadata={
                columnMetadata ? columnMetadata.concat(query.getInlineMetadata()) : undefined
              }
            />
          </div>
        </div>
        <div className="output-section">
          <div className="output-top-bar">
            <RunPreviewButton
              query={query}
              onQueryChange={onQueryChange}
              onRun={emptyQuery ? undefined : handleRun}
              onExplain={undefined}
              onExport={handleExport}
              loading={querySummaryState.loading}
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
          <div className="output-pane">
            {querySummaryState.isInit() && (
              <div className="init-placeholder">
                {emptyQuery ? (
                  <p>
                    Enter a query and click <Code>Run</Code>
                  </p>
                ) : (
                  <p>
                    Click <Code>Run</Code> to execute the query
                  </p>
                )}
              </div>
            )}
            {querySummary &&
              (querySummary.result ? (
                <QueryOutput2
                  runeMode={runeMode}
                  queryResult={querySummary.result}
                  onExport={handleExport}
                  onQueryAction={handleQueryAction}
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
                  {querySummary.stages && (
                    <TalariaStats stages={querySummary.stages} error={querySummary.error} />
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
                  onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
                  showLiveReports={showLiveReports}
                />
                {querySummaryState.intermediate.stages && showLiveReports && (
                  <TalariaStats stages={querySummaryState.intermediate.stages} />
                )}
              </div>
            )}
          </div>
        </div>
      </SplitterLayout>
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
