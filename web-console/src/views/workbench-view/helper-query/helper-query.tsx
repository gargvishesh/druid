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

import { Loader } from '../../../components';
import { usePermanentCallback, useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { TalariaHistory } from '../../../singletons/talaria-history';
import {
  Execution,
  fitExternalConfigPattern,
  LastExecution,
  summarizeExternalConfig,
  TalariaQuery,
} from '../../../talaria-models';
import { ColumnMetadata, DruidError, QueryAction, RowColumn } from '../../../utils';
import { QueryContext } from '../../../utils/query-context';
import { QueryError } from '../../query-view/query-error/query-error';
import { QueryTimer } from '../../query-view/query-timer/query-timer';
import {
  executionBackgroundStatusCheck,
  reattachAsyncExecution,
  reattachTaskExecution,
  submitAsyncQuery,
  submitTaskQuery,
} from '../execution-utils';
import { ExportDialog } from '../export-dialog/export-dialog';
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

import './helper-query.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

export interface HelperQueryProps {
  query: TalariaQuery;
  mandatoryQueryContext: QueryContext | undefined;
  columnMetadata: readonly ColumnMetadata[] | undefined;
  onQueryChange(newTalariaQuery: TalariaQuery): void;
  onDelete(): void;
  onStats(taskId: string): void;
}

export const HelperQuery = React.memo(function HelperQuery(props: HelperQueryProps) {
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
  const [executionState, queryManager] = useQueryManager<
    TalariaQuery | LastExecution,
    Execution,
    Execution,
    DruidError
  >({
    initQuery: TalariaQueryStateCache.getState(id) ? undefined : query.getLastExecution(),
    initState: TalariaQueryStateCache.getState(id),
    processQuery: async (q, cancelToken) => {
      if (q instanceof TalariaQuery) {
        TalariaQueryStateCache.deleteState(id);

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
    TalariaQueryStateCache.storeState(id, executionState);
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

    TalariaHistory.addQueryToHistory(query);
    queryManager.runQuery(preview ? query.makePreview() : query);
  }

  const collapsed = query.getCollapsed();
  const insertDatasource = query.getInsertDatasource();

  const statsTaskId: string | undefined = execution?.id;

  let extraInfo: string | undefined;
  if (collapsed && parsedQuery instanceof SqlQuery) {
    try {
      extraInfo = summarizeExternalConfig(fitExternalConfigPattern(parsedQuery));
    } catch {}
  }

  return (
    <div className="helper-query">
      <div className="query-top-bar">
        <Button
          icon={collapsed ? IconNames.CARET_RIGHT : IconNames.CARET_DOWN}
          minimal
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
          <Button
            icon={IconNames.CROSS}
            minimal
            onClick={() => {
              TalariaQueryStateCache.deleteState(id);
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
            columnMetadata={
              columnMetadata ? columnMetadata.concat(query.getInlineMetadata()) : undefined
            }
            editorStateId={query.getId()}
          />
          <div className="query-control-bar">
            <RunMoreButton
              query={query}
              onQueryChange={onQueryChange}
              onRun={handleRun}
              loading={executionState.loading}
              small
            />
            {executionState.isLoading() && <QueryTimer />}
            {(execution || executionState.error) && (
              <TalariaExtraInfo
                execution={execution}
                onExecutionDetail={() => onStats(statsTaskId!)}
                onReset={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastExecution(undefined));
                  TalariaQueryStateCache.deleteState(id);
                }}
              />
            )}
          </div>
          {!executionState.isInit() && (
            <div className="output-pane">
              {execution &&
                (execution.result ? (
                  <QueryOutput2
                    runeMode={execution.engine === 'native'}
                    queryResult={execution.result}
                    onExport={handleExport}
                    onQueryAction={handleQueryAction}
                    initPageSize={5}
                  />
                ) : execution.isSuccessfulInsert() ? (
                  <InsertSuccess
                    insertQueryExecution={execution}
                    onStats={() => onStats(statsTaskId!)}
                    onQueryChange={handleQueryStringChange}
                  />
                ) : execution.error ? (
                  <div className="stats-container">
                    <TalariaQueryError execution={execution} />
                    {execution.stages && (
                      <TalariaStats stages={execution.stages} error={execution.error} />
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
                    <StageProgress
                      execution={executionState.intermediate}
                      onCancel={() => queryManager.cancelCurrent()}
                    />
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
