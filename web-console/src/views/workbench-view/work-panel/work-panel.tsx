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

import { Icon, Intent, Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import { SqlTableRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { Loader } from '../../../components';
import { useInterval, useQueryManager } from '../../../hooks';
import { Api, AppToaster } from '../../../singletons';
import { Execution, TalariaQuery } from '../../../talaria-models';
import { deepGet, downloadQueryProfile, formatDuration, queryDruidSql } from '../../../utils';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';
import { cancelTaskExecution, getTaskExecution } from '../execution-utils';
import { ShowAsyncValueDialog } from '../show-async-value-dialog/show-async-value-dialog';
import { TalariaResultsDialog } from '../talaria-results-dialog/talaria-results-dialog';
import { useWorkStateStore } from '../work-state-store';

import './work-panel.scss';

type TaskStatus = 'RUNNING' | 'WAITING' | 'PENDING' | 'SUCCESS' | 'FAILED' | 'CANCELED';

function statusToIconAndColor(status: TaskStatus): [IconName, string] {
  switch (status) {
    case 'RUNNING':
      return [IconNames.REFRESH, '#2167d5'];
    case 'WAITING':
    case 'PENDING':
      return [IconNames.CIRCLE, '#d5631a'];
    case 'SUCCESS':
      return [IconNames.TICK_CIRCLE, '#57d500'];
    case 'FAILED':
      return [IconNames.DELETE, '#9f0d0a'];
    case 'CANCELED':
      return [IconNames.DISABLE, '#8d8d8d'];
    default:
      return [IconNames.CIRCLE, '#8d8d8d'];
  }
}

interface WorkEntry {
  taskStatus: TaskStatus;
  taskId: string;
  datasource: string;
  createdTime: string;
  duration: number;
  errorMessage?: string;
}

export interface WorkPanelProps {
  onStats(taskId: string): void;
  onRunQuery(query: string): void;
  onNewTab(talariaQuery: TalariaQuery, tabName: string): void;
}

export const WorkPanel = React.memo(function WorkPanel(props: WorkPanelProps) {
  const { onStats, onRunQuery, onNewTab } = props;

  const [loadValue, setLoadValue] = useState<
    { title: string; work: () => Promise<string> } | undefined
  >();
  const [confirmCancelId, setConfirmCancelId] = useState<string | undefined>();

  const [resultsTaskId, setResultsTaskId] = useState<string | undefined>();

  const workStateVersion = useWorkStateStore(state => state.version);

  const [workQueryState, queryManager] = useQueryManager<number, WorkEntry[]>({
    query: workStateVersion,
    processQuery: async _ => {
      return await queryDruidSql<WorkEntry>({
        query: `SELECT
  CASE WHEN "error_msg" = 'Shutdown request from user' THEN 'CANCELED' ELSE "status" END AS "taskStatus",
  "task_id" AS "taskId",
  "datasource",
  "created_time" AS "createdTime",
  "duration",
  "error_msg" AS "errorMessage"
FROM sys.tasks
WHERE "type" = 'talaria0'
ORDER BY "created_time" DESC
LIMIT 100`,
      });
    },
  });

  useInterval(() => {
    queryManager.rerunLastQuery(true);
  }, 30000);

  const incrementWorkVersion = useWorkStateStore(state => state.increment);

  const workEntries = workQueryState.getSomeData();
  return (
    <div className="work-panel">
      <div className="title">Work history</div>
      {workEntries ? (
        <div className="work-entries">
          {workEntries.length === 0 && <div className="empty-placeholder">No recent queries</div>}
          {workEntries.map(w => {
            const menu = (
              <Menu>
                <MenuItem
                  text="Attach in new tab"
                  onClick={async () => {
                    let execution: Execution;
                    try {
                      execution = await getTaskExecution(w.taskId);
                    } catch {
                      AppToaster.show({
                        message: 'Could not get payload',
                        intent: Intent.DANGER,
                      });
                      return;
                    }

                    if (!execution.sqlQuery || !execution.queryContext) {
                      AppToaster.show({
                        message: 'Could not get query',
                        intent: Intent.DANGER,
                      });
                      return;
                    }

                    onNewTab(
                      TalariaQuery.fromEffectiveQueryAndContext(
                        execution.sqlQuery,
                        execution.queryContext,
                      )
                        .explodeQuery()
                        .changeLastExecution({ engine: 'sql-task', id: w.taskId }),
                      'Attached',
                    );
                  }}
                />
                <MenuItem
                  text="Copy ID"
                  onClick={() => {
                    copy(w.taskId, { format: 'text/plain' });
                    AppToaster.show({
                      message: `${w.taskId} copied to clipboard`,
                      intent: Intent.SUCCESS,
                    });
                  }}
                />
                <MenuDivider />
                <MenuItem
                  text="Show stats"
                  onClick={() => {
                    onStats(w.taskId);
                  }}
                />
                <MenuItem
                  text="Show SQL query"
                  onClick={() => {
                    setLoadValue({
                      title: 'SQL query',
                      work: async () => {
                        const execution = await getTaskExecution(w.taskId);
                        if (!execution.sqlQuery) throw new Error('Could not get query');
                        return execution.sqlQuery;
                      },
                    });
                  }}
                />
                <MenuItem
                  text="Show native query"
                  onClick={() => {
                    setLoadValue({
                      title: 'Native query',
                      work: async () => {
                        const payloadResp = await Api.instance.get(
                          `/druid/indexer/v1/task/${Api.encodePath(w.taskId)}`,
                        );
                        const nativeQuery = deepGet(payloadResp, 'data.payload.spec.query');
                        return nativeQuery
                          ? JSONBig.stringify(nativeQuery, undefined, 2)
                          : 'Could not extract native query';
                      },
                    });
                  }}
                />
                {w.taskStatus === 'SUCCESS' &&
                  (w.datasource === TalariaQuery.INLINE_DATASOURCE_MARKER ? (
                    <MenuItem text="Show results" onClick={() => setResultsTaskId(w.taskId)} />
                  ) : (
                    <MenuItem
                      text={`SELECT * FROM ${SqlTableRef.create(w.datasource)}`}
                      onClick={() =>
                        onRunQuery(`SELECT * FROM ${SqlTableRef.create(w.datasource)}`)
                      }
                    />
                  ))}
                {w.taskStatus === 'FAILED' && (
                  <MenuItem
                    text="Show error"
                    onClick={() => {
                      setLoadValue({
                        title: 'Query error',
                        work: async () => {
                          const execution = await getTaskExecution(w.taskId);
                          const { error } = execution;
                          if (!error) return String(w.errorMessage);
                          return `${error.error.errorCode}: ${
                            error.error.errorMessage ||
                            (error.exceptionStackTrace || '').split('\n')[0]
                          }`;
                        },
                      });
                    }}
                  />
                )}
                <MenuItem
                  text="Download query profile"
                  onClick={() => downloadQueryProfile(w.taskId)}
                />
                {w.taskStatus === 'RUNNING' && (
                  <>
                    <MenuDivider />
                    <MenuItem
                      text="Cancel query"
                      intent={Intent.DANGER}
                      onClick={() => setConfirmCancelId(w.taskId)}
                    />
                  </>
                )}
              </Menu>
            );

            const [icon, color] = statusToIconAndColor(w.taskStatus);
            return (
              <Popover2 className="work-entry" key={w.taskId} position="left" content={menu}>
                <div title={w.errorMessage}>
                  <div className="line1">
                    <Icon
                      className={'status-icon ' + w.taskStatus.toLowerCase()}
                      icon={icon}
                      style={{ color }}
                    />
                    <div className="timing">
                      {w.createdTime.replace('T', ' ').replace(/\.\d\d\dZ$/, '') +
                        (w.duration > 0 ? ` (${formatDuration(w.duration)})` : '')}
                    </div>
                  </div>
                  <div className="line2">
                    <Icon
                      className="output-icon"
                      icon={
                        w.datasource === TalariaQuery.INLINE_DATASOURCE_MARKER
                          ? IconNames.APPLICATION
                          : IconNames.CLOUD_UPLOAD
                      }
                    />
                    <div
                      className={classNames('output-datasource', {
                        query: w.datasource === TalariaQuery.INLINE_DATASOURCE_MARKER,
                      })}
                    >
                      {w.datasource === TalariaQuery.INLINE_DATASOURCE_MARKER
                        ? 'data in report'
                        : w.datasource}
                    </div>
                  </div>
                </div>
              </Popover2>
            );
          })}
        </div>
      ) : workQueryState.isLoading() ? (
        <Loader />
      ) : undefined}
      {loadValue && (
        <ShowAsyncValueDialog
          title={loadValue.title}
          loadValue={loadValue.work}
          onClose={() => setLoadValue(undefined)}
        />
      )}
      {resultsTaskId && (
        <TalariaResultsDialog id={resultsTaskId} onClose={() => setResultsTaskId(undefined)} />
      )}
      {confirmCancelId && (
        <CancelQueryDialog
          onCancel={async () => {
            if (!confirmCancelId) return;
            try {
              await cancelTaskExecution(confirmCancelId);
              AppToaster.show({
                message: 'Query canceled',
                intent: Intent.SUCCESS,
              });
              incrementWorkVersion();
            } catch {
              AppToaster.show({
                message: 'Could not cancel query',
                intent: Intent.DANGER,
              });
            }
          }}
          onDismiss={() => setConfirmCancelId(undefined)}
        />
      )}
    </div>
  );
});
