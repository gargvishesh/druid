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

import { Loader } from '../../../components';
import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { TalariaHistory } from '../../../singletons/talaria-history';
import { QueryExecution, TalariaQuery } from '../../../talaria-models';
import {
  ColumnMetadata,
  DruidError,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  QueryAction,
  RowColumn,
} from '../../../utils';
import { QueryContext } from '../../../utils/query-context';
import { QueryError } from '../../query-view/query-error/query-error';
import { AnchoredQueryTimer } from '../anchored-query-timer/anchored-query-timer';
import {
  reattachAsyncQuery,
  submitAsyncQuery,
  talariaBackgroundStatusCheck,
} from '../execution-utils';
import { ExportDialog } from '../export-dialog/export-dialog';
import { HelperQuery } from '../helper-query/helper-query';
import { InsertSuccess } from '../insert-success/insert-success';
import { useMetadataStateStore } from '../metadata-state-store';
import { QueryOutput2 } from '../query-output2/query-output2';
import { RunMoreButton } from '../run-more-button/run-more-button';
import { StageProgress } from '../stage-progress/stage-progress';
import { TalariaExtraInfo } from '../talaria-extra-info/talaria-extra-info';
import { TalariaQueryError } from '../talaria-query-error/talaria-query-error';
import { TalariaQueryInput } from '../talaria-query-input/talaria-query-input';
import { TalariaQueryStateCache } from '../talaria-query-state-cache';
import { TalariaStats } from '../talaria-stats/talaria-stats';
import { useWorkStateStore } from '../work-state-store';

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
    localStorageSet(LocalStorageKeys.TALARIA_TAB_PANE_SIZE, String(secondaryPaneSize));
  }, []);

  const queryInputRef = useRef<TalariaQueryInput | null>(null);

  const id = query.getId();
  const [queryExecutionState, queryManager] = useQueryManager<
    TalariaQuery | string,
    QueryExecution,
    QueryExecution,
    DruidError
  >({
    initQuery: TalariaQueryStateCache.getState(id) ? undefined : query.getLastQueryId(),
    initState: TalariaQueryStateCache.getState(id),
    processQuery: async (q: TalariaQuery | string, cancelToken) => {
      if (q instanceof TalariaQuery) {
        TalariaQueryStateCache.deleteState(id);

        const {
          query,
          context,
          prefixLines,
          isAsync,
          isSql,
          cancelQueryId,
        } = q.getEffectiveQueryAndContext();

        if (isAsync) {
          if (!isSql) throw new Error('must be SQL to be async');
          return await submitAsyncQuery({
            query,
            context,
            prefixLines,
            cancelToken,
            preserveOnTermination: true,
            onSubmitted: id => {
              onQueryChange(props.query.changeLastQueryId(id));
            },
          });
        } else {
          if (cancelQueryId) {
            void cancelToken.promise
              .then(() => {
                return Api.instance.delete(
                  `/druid/v2${isSql ? '/sql' : ''}/${Api.encodePath(cancelQueryId)}`,
                );
              })
              .catch(() => {});
          }

          const queryContext = { ...context, ...(mandatoryQueryContext || {}) };
          let result: QueryResult;
          try {
            result = await queryRunner.runQuery({
              query,
              extraQueryContext: queryContext,
              cancelToken,
            });
          } catch (e) {
            onQueryChange(props.query.changeLastQueryId(undefined));
            throw new DruidError(e, prefixLines);
          }

          onQueryChange(props.query.changeLastQueryId(undefined));
          return QueryExecution.fromResult(result);
        }
      } else {
        return await reattachAsyncQuery({
          id: q,
          cancelToken,
          preserveOnTermination: true,
        });
      }
    },
    backgroundStatusCheck: talariaBackgroundStatusCheck,
  });

  useEffect(() => {
    if (queryExecutionState.data || queryExecutionState.error) {
      TalariaQueryStateCache.storeState(id, queryExecutionState);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryExecutionState.data, queryExecutionState.error]);

  const incrementWorkVersion = useWorkStateStore(state => state.increment);
  useEffect(() => {
    incrementWorkVersion();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryExecutionState.loading]);

  const queryExecution = queryExecutionState.data;

  const incrementMetadataVersion = useMetadataStateStore(state => state.increment);
  useEffect(() => {
    if (queryExecution?.isSuccessfulInsert()) {
      incrementMetadataVersion();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Boolean(queryExecution?.isSuccessfulInsert())]);

  function moveToPosition(position: RowColumn) {
    const currentQueryInput = queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  function handleRun(preview = true) {
    if (query.isJsonLike() && !query.validRune()) return;

    TalariaHistory.addQueryToHistory(query);
    queryManager.runQuery(preview ? query.makePreview() : query);
  }

  const runeMode = query.isJsonLike();

  const statsTaskId: string | undefined = queryExecution?.id;

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
        <div className="top-section">
          <div className="query-section">
            {queryPrefixes.map((queryPrefix, i) => (
              <HelperQuery
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
              <TalariaQueryInput
                ref={queryInputRef}
                autoHeight={Boolean(queryPrefixes.length)}
                minRows={10}
                queryString={query.getQueryString()}
                onQueryStringChange={handleQueryStringChange}
                runeMode={runeMode}
                columnMetadata={
                  columnMetadata ? columnMetadata.concat(query.getInlineMetadata()) : undefined
                }
                editorStateId={query.getId()}
              />
              <ButtonGroup className="corner">
                <Button
                  icon={IconNames.ARROW_UP}
                  title="Save as helper query"
                  minimal
                  onClick={() => {
                    onQueryChange(query.addBlank());
                  }}
                />
                <Popover2
                  content={
                    <Menu>
                      <MenuItem
                        icon={IconNames.ARROW_UP}
                        text="Save as helper query"
                        onClick={() => {
                          onQueryChange(query.addBlank());
                        }}
                      />
                      <MenuItem
                        icon={IconNames.DUPLICATE}
                        text="Duplicate"
                        onClick={() => onQueryChange(query.duplicateLast())}
                      />
                    </Menu>
                  }
                >
                  <Button icon={IconNames.MORE} minimal />
                </Popover2>
              </ButtonGroup>
            </div>
          </div>
          <div className="run-bar">
            <RunMoreButton
              query={query}
              onQueryChange={onQueryChange}
              onRun={handleRun}
              onExport={handleExport}
              loading={queryExecutionState.loading}
            />
            {queryExecutionState.isLoading() && (
              <AnchoredQueryTimer startTime={queryExecutionState.intermediate?.startTime} />
            )}
            {queryExecution?.result && (
              <TalariaExtraInfo
                queryExecution={queryExecution}
                onStats={() => onStats(statsTaskId!)}
              />
            )}
            {(queryExecution || queryExecutionState.error) && (
              <Button
                className="reset"
                icon={IconNames.CROSS}
                minimal
                onClick={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastQueryId(undefined));
                  TalariaQueryStateCache.deleteState(id);
                }}
              />
            )}
          </div>
        </div>
        <div className="output-section">
          {queryExecutionState.isInit() && (
            <div className="init-placeholder">
              {query.isEmptyQuery() ? (
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
          {queryExecution &&
            (queryExecution.result ? (
              <QueryOutput2
                runeMode={runeMode}
                queryResult={queryExecution.result}
                onExport={handleExport}
                onQueryAction={handleQueryAction}
              />
            ) : queryExecution.isSuccessfulInsert() ? (
              <InsertSuccess
                insertQueryExecution={queryExecution}
                onStats={() => onStats(statsTaskId!)}
                onQueryChange={handleQueryStringChange}
              />
            ) : queryExecution.error ? (
              <div className="stats-container">
                <TalariaQueryError taskError={queryExecution.error} />
                {queryExecution.stages && (
                  <TalariaStats stages={queryExecution.stages} error={queryExecution.error} />
                )}
              </div>
            ) : (
              <div>Unknown query execution state</div>
            ))}
          {queryExecutionState.error && (
            <QueryError
              error={queryExecutionState.error}
              moveCursorTo={position => {
                moveToPosition(position);
              }}
              queryString={query.getQueryString()}
              onQueryStringChange={handleQueryStringChange}
            />
          )}
          {queryExecutionState.isLoading() &&
            (queryExecutionState.intermediate ? (
              <div className="stats-container">
                <StageProgress
                  queryExecution={queryExecutionState.intermediate}
                  onCancel={() => queryManager.cancelCurrent()}
                  onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
                  showLiveReports={showLiveReports}
                />
                {queryExecutionState.intermediate.stages && showLiveReports && (
                  <TalariaStats stages={queryExecutionState.intermediate.stages} />
                )}
              </div>
            ) : (
              <Loader
                cancelText="Cancel query"
                onCancel={() => {
                  queryManager.cancelCurrent();
                }}
              />
            ))}
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
