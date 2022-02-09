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
  fitExternalConfigPattern,
  QueryExecution,
  summarizeExternalConfig,
  TalariaQuery,
} from '../../../talaria-models';
import { ColumnMetadata, DruidError, QueryAction, RowColumn } from '../../../utils';
import { QueryContext } from '../../../utils/query-context';
import { QueryError } from '../../query-view/query-error/query-error';
import { QueryTimer } from '../../query-view/query-timer/query-timer';
import {
  reattachAsyncQuery,
  submitAsyncQuery,
  talariaBackgroundStatusCheck,
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

const queryRunner = new QueryRunner();

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

        const { query, context, prefixLines, isAsync, isSql, cancelQueryId } =
          q.getEffectiveQueryAndContext();

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
  const collapsed = query.getCollapsed();
  const insertDatasource = query.getInsertDatasource();

  const statsTaskId: string | undefined = queryExecution?.id;

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
            runeMode={runeMode}
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
              onExport={handleExport}
              loading={queryExecutionState.loading}
              small
            />
            {queryExecutionState.isLoading() && <QueryTimer />}
            {queryExecution?.result && (
              <TalariaExtraInfo
                queryResult={queryExecution.result}
                onStats={() => onStats(statsTaskId!)}
              />
            )}
            {(queryExecution || queryExecutionState.error) && (
              <Button
                className="reset"
                icon={IconNames.CROSS}
                minimal
                small
                onClick={() => {
                  queryManager.reset();
                  onQueryChange(props.query.changeLastQueryId(undefined));
                  TalariaQueryStateCache.deleteState(id);
                }}
              />
            )}
          </div>
          {!queryExecutionState.isInit() && (
            <div className="output-pane">
              {queryExecution &&
                (queryExecution.result ? (
                  <QueryOutput2
                    runeMode={runeMode}
                    queryResult={queryExecution.result}
                    onExport={handleExport}
                    onQueryAction={handleQueryAction}
                    initPageSize={5}
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
                      stages={queryExecutionState.intermediate.stages}
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
