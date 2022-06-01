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

import { Button, Classes, Dialog, Intent, Switch, Tab, Tabs, TextArea } from '@blueprintjs/core';
import { Popover2 } from '@blueprintjs/popover2';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { CenterMessage } from '../../../components';
import {
  WorkbenchHistory,
  WorkbenchQueryHistoryEntry,
} from '../../../singletons/workbench-history';
import { pluralIfNeeded } from '../../../utils';
import { WorkbenchQuery } from '../../../workbench-models';
import { ExecutionStagesPaneLoader } from '../execution-stages-pane-loader/execution-stages-pane-loader';

import './workbench-history-dialog.scss';

export interface WorkbenchHistoryDialogProps {
  onSelectQuery(query: WorkbenchQuery): void;
  onClose(): void;
}

export const WorkbenchHistoryDialog = React.memo(function WorkbenchHistoryDialog(
  props: WorkbenchHistoryDialogProps,
) {
  const { onSelectQuery, onClose } = props;
  const [activeTab, setActiveTab] = useState(0);
  const [showStats, setShowStats] = useState(false);
  const [queryRecords] = useState(() => WorkbenchHistory.getHistory());

  function handleSelect() {
    const queryRecord = queryRecords[activeTab];
    onSelectQuery(queryRecord.query);
    onClose();
  }

  function renderQueryEntry(record: WorkbenchQueryHistoryEntry) {
    return (
      <div className="query-entry">
        <div className="query-info-bar">
          <Popover2
            content={
              <pre className="json-popover-content">
                {JSONBig.stringify(record.query.queryContext, undefined, 2)}
              </pre>
            }
          >
            <Button
              text={`Context: ${pluralIfNeeded(
                Object.keys(record.query.queryContext).length,
                'key',
              )}`}
              minimal
              small
            />
          </Popover2>
          <Switch
            label="Show stats"
            checked={Boolean(showStats && record.taskId)}
            onChange={() => setShowStats(!showStats)}
            disabled={!record.taskId}
          />
        </div>
        <TextArea className="query-string" readOnly value={record.query.getQueryString()} />
        {showStats && record.taskId && <ExecutionStagesPaneLoader id={record.taskId} />}
      </div>
    );
  }

  function renderContent(): JSX.Element {
    if (!queryRecords.length) {
      return <CenterMessage>The query history is empty.</CenterMessage>;
    }

    return (
      <Tabs
        className="version-tabs"
        animate
        renderActiveTabPanelOnly
        vertical
        selectedTabId={activeTab}
        onChange={(t: number) => setActiveTab(t)}
      >
        {queryRecords.map((record, index) => (
          <Tab
            id={index}
            key={index}
            title={record.version}
            panel={renderQueryEntry(record)}
            panelClassName="panel"
          />
        ))}
        <Tabs.Expander />
      </Tabs>
    );
  }

  return (
    <Dialog className="workbench-history-dialog" isOpen onClose={onClose} title="Query history">
      <div className={Classes.DIALOG_BODY}>{renderContent()}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          {Boolean(queryRecords.length) && (
            <Button text="Open in new tab" intent={Intent.PRIMARY} onClick={handleSelect} />
          )}
        </div>
      </div>
    </Dialog>
  );
});
