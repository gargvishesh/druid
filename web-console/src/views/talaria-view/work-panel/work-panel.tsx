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
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { SqlTableRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { Loader } from '../../../components';
import { useInterval, useQueryManager } from '../../../hooks';
import { Api, AppToaster } from '../../../singletons';
import { TalariaQuery } from '../../../talaria-models';
import { formatDuration, queryDruidSql } from '../../../utils';
import { ShowAsyncValueDialog } from '../show-async-value-dialog/show-async-value-dialog';
import { TalariaResultsDialog } from '../talaria-results-dialog/talaria-results-dialog';
import { useWorkStateStore } from '../work-state-store';

import './work-panel.scss';

const REPORT_DATASOURCE = '__you_have_been_visited_by_talaria';

function statusToColor(status: TaskStatus): string {
  switch (status) {
    case 'RUNNING':
      return '#2167d5';
    case 'WAITING':
      return '#d5631a';
    case 'PENDING':
      return '#ffbf00';
    case 'SUCCESS':
      return '#57d500';
    case 'FAILED':
      return '#d5100a';
    default:
      return '#0a1500';
  }
}

type TaskStatus = 'RUNNING' | 'WAITING' | 'PENDING' | 'SUCCESS' | 'FAILED';

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

  const [resultsTaskId, setResultsTaskId] = useState<string | undefined>();

  const workStateVersion = useWorkStateStore(state => state.version);

  const [workQueryState, queryManager] = useQueryManager<number, WorkEntry[]>({
    query: workStateVersion,
    processQuery: async _ => {
      return await queryDruidSql<WorkEntry>({
        query: `SELECT
  "status" AS "taskStatus",
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
          {workEntries.map(w => {
            const menu = (
              <Menu>
                <MenuItem
                  text="Attach in new tab"
                  onClick={async () => {
                    try {
                      const payloadResp = await Api.instance.get(
                        `/druid/indexer/v1/task/${Api.encodePath(w.taskId)}`,
                      );
                      const payload = payloadResp.data.payload;
                      onNewTab(
                        TalariaQuery.fromEffectiveQueryAndContext(
                          payload.sqlQuery,
                          payload.sqlQueryContext,
                        )
                          .explodeQuery()
                          .changeLastQueryId(w.taskId),
                        'Attached',
                      );
                    } catch {
                      AppToaster.show({
                        message: 'Could not get payload',
                        intent: Intent.DANGER,
                      });
                    }
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
                        const payloadResp = await Api.instance.get(
                          `/druid/indexer/v1/task/${Api.encodePath(w.taskId)}`,
                        );
                        return payloadResp.data.payload.sqlQuery;
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
                        return JSONBig.stringify(payloadResp.data.payload.spec.query, undefined, 2);
                      },
                    });
                  }}
                />
                {w.taskStatus === 'SUCCESS' &&
                  (w.datasource === REPORT_DATASOURCE ? (
                    <MenuItem text="Show results" onClick={() => setResultsTaskId(w.taskId)} />
                  ) : (
                    <MenuItem
                      text={`SELECT * FROM ${SqlTableRef.create(w.datasource)}`}
                      onClick={() =>
                        onRunQuery(`SELECT * FROM ${SqlTableRef.create(w.datasource)}`)
                      }
                    />
                  ))}
                {w.taskStatus === 'RUNNING' && (
                  <>
                    <MenuDivider />
                    <MenuItem
                      text="Cancel task"
                      intent={Intent.DANGER}
                      onClick={async () => {
                        try {
                          await Api.instance.post(
                            `/druid/indexer/v1/task/${Api.encodePath(w.taskId)}/shutdown`,
                            {},
                          );
                          AppToaster.show({
                            message: 'Task canceled',
                            intent: Intent.SUCCESS,
                          });
                          incrementWorkVersion();
                        } catch {
                          AppToaster.show({
                            message: 'Could not cancel task',
                            intent: Intent.DANGER,
                          });
                        }
                      }}
                    />
                  </>
                )}
              </Menu>
            );

            return (
              <Popover2 className="work-entry" key={w.taskId} position="left" content={menu}>
                <div title={w.errorMessage}>
                  <div className="line1">
                    <div className="status-dot" style={{ color: statusToColor(w.taskStatus) }}>
                      &#x25cf;
                    </div>
                    <div className="timing">
                      {w.createdTime.replace('T', ' ').replace(/\.\d\d\dZ$/, '') +
                        ' (' +
                        (w.duration >= 0 ? formatDuration(w.duration) : 'n/a') +
                        ')'}
                    </div>
                  </div>
                  <div className="line2">
                    <Icon
                      className="output-icon"
                      icon={
                        w.datasource === REPORT_DATASOURCE
                          ? IconNames.APPLICATION
                          : IconNames.CLOUD_UPLOAD
                      }
                    />
                    <div
                      className={classNames('output-datasource', {
                        query: w.datasource === REPORT_DATASOURCE,
                      })}
                    >
                      {w.datasource === REPORT_DATASOURCE ? 'data in report' : w.datasource}
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
    </div>
  );
});
