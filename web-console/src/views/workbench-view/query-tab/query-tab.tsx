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
import { WorkbenchHistory } from '../../../singletons/workbench-history';
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
import { DruidEngine, Execution, LastExecution, WorkbenchQuery } from '../../../workbench-models';
import { QueryError } from '../../query-view/query-error/query-error';
import { ExecutionDetailsTab } from '../execution-details-pane/execution-details-pane';
import { ExecutionErrorPane } from '../execution-error-pane/execution-error-pane';
import { ExecutionStagesPane } from '../execution-stages-pane/execution-stages-pane';
import { ExecutionStateCache } from '../execution-state-cache';
import { ExecutionSummaryPanel } from '../execution-summary-panel/execution-summary-panel';
import { ExecutionTimerPanel } from '../execution-timer-panel/execution-timer-panel';
import {
  executionBackgroundStatusCheck,
  reattachAsyncExecution,
  reattachTaskExecution,
  submitAsyncQuery,
  submitTaskQuery,
} from '../execution-utils';
import { ExportDialog } from '../export-dialog/export-dialog';
import { FlexibleQueryInput } from '../flexible-query-input/flexible-query-input';
import { HelperQuery } from '../helper-query/helper-query';
import { InsertSuccessPane } from '../insert-success-pane/insert-success-pane';
import { useMetadataStateStore } from '../metadata-state-store';
import { QueryOutput2 } from '../query-output2/query-output2';
import { RunPanel } from '../run-panel/run-panel';
import { StateProgressPane } from '../state-progress-pane/state-progress-pane';
import { useWorkStateStore } from '../work-state-store';

import './query-tab.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

export interface QueryTabProps {
  query: WorkbenchQuery;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newQuery: WorkbenchQuery): void;
  onDetails(id: string, initTab?: ExecutionDetailsTab): void;
  extraEngines: DruidEngine[];
}

export const QueryTab = React.memo(function QueryTab(props: QueryTabProps) {
  const { query, columnMetadata, mandatoryQueryContext, onQueryChange, onDetails, extraEngines } =
    props;
  const [showLiveReports, setShowLiveReports] = useState(true);
  const [exportDialogQuery, setExportDialogQuery] = useState<WorkbenchQuery | undefined>();

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
    localStorageSet(LocalStorageKeys.WORKBENCH_PANE_SIZE, String(secondaryPaneSize));
  }, []);

  const queryInputRef = useRef<FlexibleQueryInput | null>(null);

  const id = query.getId();
  const [executionState, queryManager] = useQueryManager<
    WorkbenchQuery | LastExecution,
    Execution,
    Execution,
    DruidError
  >({
    initQuery: ExecutionStateCache.getState(id) ? undefined : query.getLastExecution(),
    initState: ExecutionStateCache.getState(id),
    processQuery: async (q, cancelToken) => {
      if (q instanceof WorkbenchQuery) {
        ExecutionStateCache.deleteState(id);
        const { engine, query, sqlPrefixLines, cancelQueryId } = q.getApiQuery();

        switch (engine) {
          case 'sql-async':
            return await submitAsyncQuery({
              query,
              prefixLines: sqlPrefixLines,
              cancelToken,
              preserveOnTermination: true,
              onSubmitted: id => {
                onQueryChange(props.query.changeLastExecution({ engine, id }));
              },
            });

          case 'sql-task':
            return await submitTaskQuery({
              query,
              prefixLines: sqlPrefixLines,
              cancelToken,
              preserveOnTermination: true,
              onSubmitted: id => {
                onQueryChange(props.query.changeLastExecution({ engine, id }));
              },
            });

          case 'native':
          case 'sql': {
            if (cancelQueryId) {
              void cancelToken.promise
                .then(() => {
                  return Api.instance.delete(
                    `/druid/v2${engine === 'sql' ? '/sql' : ''}/${Api.encodePath(cancelQueryId)}`,
                  );
                })
                .catch(() => {});
            }

            let result: QueryResult;
            try {
              result = await queryRunner.runQuery({
                query,
                extraQueryContext: mandatoryQueryContext,
                cancelToken,
              });
            } catch (e) {
              onQueryChange(props.query.changeLastExecution(undefined));
              throw new DruidError(e, sqlPrefixLines);
            }

            onQueryChange(props.query.changeLastExecution(undefined));
            return Execution.fromResult(engine, result);
          }
        }
      } else {
        switch (q.engine) {
          case 'sql-task':
            return await reattachTaskExecution({
              id: q.id,
              cancelToken,
              preserveOnTermination: true,
            });

          default:
            return await reattachAsyncExecution({
              id: q.id,
              cancelToken,
              preserveOnTermination: true,
            });
        }
      }
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
  });

  useEffect(() => {
    if (!executionState.data) return;
    ExecutionStateCache.storeState(id, executionState);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.data, executionState.error]);

  const incrementWorkVersion = useWorkStateStore(state => state.increment);
  useEffect(() => {
    incrementWorkVersion();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [executionState.loading]);

  const execution = executionState.data;

  const incrementMetadataVersion = useMetadataStateStore(state => state.increment);
  useEffect(() => {
    if (execution?.isSuccessfulInsert()) {
      incrementMetadataVersion();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Boolean(execution?.isSuccessfulInsert())]);

  function moveToPosition(position: RowColumn) {
    const currentQueryInput = queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  function handleRun(preview = true) {
    if (!query.isValid()) return;

    WorkbenchHistory.addQueryToHistory(query);
    queryManager.runQuery(preview ? query.makePreview() : query);
  }

  const statsTaskId: string | undefined = execution?.id;

  const queryPrefixes = query.getPrefixQueries();

  return (
    <div className="query-tab">
      <SplitterLayout
        vertical
        percentage
        secondaryInitialSize={Number(localStorageGet(LocalStorageKeys.WORKBENCH_PANE_SIZE)!) || 40}
        primaryMinSize={20}
        secondaryMinSize={20}
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
                onDetails={onDetails}
                extraEngines={extraEngines}
              />
            ))}
            <div className={classNames('main-query', queryPrefixes.length ? 'multi' : 'single')}>
              <FlexibleQueryInput
                ref={queryInputRef}
                autoHeight={Boolean(queryPrefixes.length)}
                minRows={10}
                queryString={query.getQueryString()}
                onQueryStringChange={handleQueryStringChange}
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
            <RunPanel
              query={query}
              onQueryChange={onQueryChange}
              onRun={handleRun}
              loading={executionState.loading}
              extraEngines={extraEngines}
            />
            {executionState.isLoading() && (
              <ExecutionTimerPanel
                execution={executionState.intermediate}
                onCancel={() => queryManager.cancelCurrent()}
              />
            )}
            {(execution || executionState.error) && (
              <ExecutionSummaryPanel
                execution={execution}
                onExecutionDetail={() => onDetails(statsTaskId!)}
                onReset={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastExecution(undefined));
                  ExecutionStateCache.deleteState(id);
                }}
              />
            )}
          </div>
        </div>
        <div className="output-section">
          {executionState.isInit() && (
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
          {execution &&
            (execution.result ? (
              <QueryOutput2
                runeMode={execution.engine === 'native'}
                queryResult={execution.result}
                onExport={handleExport}
                onQueryAction={handleQueryAction}
              />
            ) : execution.isSuccessfulInsert() ? (
              <InsertSuccessPane
                execution={execution}
                onDetails={() => onDetails(statsTaskId!)}
                onQueryChange={handleQueryStringChange}
              />
            ) : execution.error ? (
              <div className="stats-container">
                <ExecutionErrorPane execution={execution} />
                {execution.stages && (
                  <ExecutionStagesPane
                    execution={execution}
                    onErrorClick={() => onDetails(statsTaskId!, 'error')}
                    onWarningClick={() => onDetails(statsTaskId!, 'warnings')}
                  />
                )}
              </div>
            ) : (
              <div>Unknown query execution state</div>
            ))}
          {executionState.error && (
            <QueryError
              error={executionState.error}
              moveCursorTo={position => {
                moveToPosition(position);
              }}
              queryString={query.getQueryString()}
              onQueryStringChange={handleQueryStringChange}
            />
          )}
          {executionState.isLoading() &&
            (executionState.intermediate ? (
              <div className="stats-container">
                <StateProgressPane
                  execution={executionState.intermediate}
                  onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
                  showLiveReports={showLiveReports}
                />
                {executionState.intermediate.stages && showLiveReports && (
                  <ExecutionStagesPane execution={executionState.intermediate} />
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
          query={exportDialogQuery}
          onClose={() => {
            setExportDialogQuery(undefined);
          }}
        />
      )}
    </div>
  );
});
