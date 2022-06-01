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

import { Button, ButtonGroup, Intent, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { SqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { SpecDialog } from '../../dialogs';
import { AppToaster } from '../../singletons';
import { AceEditorStateCache } from '../../singletons/ace-editor-state-cache';
import {
  ColumnMetadata,
  deepSet,
  localStorageGet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSet,
  localStorageSetJson,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';
import { DruidEngine, generate8HexId, TabEntry, WorkbenchQuery } from '../../workbench-models';
import { ColumnTree } from '../query-view/column-tree/column-tree';
import {
  LIVE_QUERY_MODES,
  LiveQueryMode,
} from '../query-view/live-query-mode-selector/live-query-mode-selector';

import { getDemoQueries } from './demo-queries';
import { ExecutionDetailsDialog } from './execution-details-dialog/execution-details-dialog';
import { ExecutionStateCache } from './execution-state-cache';
import { ExternalConfigDialog } from './external-config-dialog/external-config-dialog';
import { MetadataChangeDetector } from './metadata-change-detector';
import { QueryTab } from './query-tab/query-tab';
import { convertSpecToSql, getSpecDatasourceName } from './spec-conversion';
import { TabRenameDialog } from './tab-rename-dialog/tab-rename-dialog';
import { WorkPanel } from './work-panel/work-panel';
import { WorkbenchHistoryDialog } from './workbench-history-dialog/workbench-history-dialog';

import './workbench-view.scss';

function cleanupTabEntry(tabEntry: TabEntry): void {
  const discardedIds = tabEntry.query.getIds();
  ExecutionStateCache.deleteStates(discardedIds);
  AceEditorStateCache.deleteStates(discardedIds);
}

export interface WorkbenchViewProps {
  tabId: string | undefined;
  onTabChange(newTabId: string): void;
  initQuery: string | undefined;
  defaultQueryContext?: Record<string, any>;
  mandatoryQueryContext?: Record<string, any>;
  extraEngines: DruidEngine[];
}

export interface WorkbenchViewState {
  tabEntries: TabEntry[];
  liveQueryMode: LiveQueryMode;

  columnMetadataState: QueryState<readonly ColumnMetadata[]>;

  initExternalConfig: boolean;
  queryStatsId?: string;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  specDialogOpen: boolean;
  renamingTab?: TabEntry;

  showWorkHistory: boolean;
}

export class WorkbenchView extends React.PureComponent<WorkbenchViewProps, WorkbenchViewState> {
  private readonly metadataQueryManager: QueryManager<null, ColumnMetadata[]>;

  constructor(props: WorkbenchViewProps, context: any) {
    super(props, context);

    const possibleTabEntries: TabEntry[] = localStorageGetJson(LocalStorageKeys.WORKBENCH_QUERIES);
    const possibleLiveQueryMode = localStorageGetJson(LocalStorageKeys.WORKBENCH_LIVE_MODE);
    const liveQueryMode = LIVE_QUERY_MODES.includes(possibleLiveQueryMode)
      ? possibleLiveQueryMode
      : 'auto';

    const showWorkHistory = Boolean(
      props.extraEngines.includes('sql-task') &&
        localStorageGetJson(LocalStorageKeys.WORKBENCH_WORK_PANEL),
    );

    const tabEntries =
      Array.isArray(possibleTabEntries) && possibleTabEntries.length
        ? possibleTabEntries.map(q => ({ ...q, query: new WorkbenchQuery(q.query) }))
        : [];

    const { initQuery } = props;
    if (initQuery) {
      // Put it in the front so that it is the opened tab
      tabEntries.unshift({
        id: generate8HexId(),
        tabName: 'Opened query',
        query: WorkbenchQuery.blank()
          .changeQueryString(initQuery)
          .changeQueryContext(props.defaultQueryContext || {}),
      });
    }

    if (!tabEntries.length) {
      tabEntries.push({
        id: generate8HexId(),
        tabName: 'Tab 1',
        query: WorkbenchQuery.blank().changeQueryContext(props.defaultQueryContext || {}),
      });
    }

    this.state = {
      tabEntries,
      liveQueryMode,

      columnMetadataState: QueryState.INIT,

      editContextDialogOpen: false,
      historyDialogOpen: false,
      specDialogOpen: false,
      initExternalConfig: false,

      showWorkHistory,
    };

    this.metadataQueryManager = new QueryManager({
      processQuery: async () => {
        return await queryDruidSql<ColumnMetadata>({
          query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS`,
        });
      },
      onStateChange: columnMetadataState => {
        if (columnMetadataState.error) {
          AppToaster.show({
            message: 'Could not load SQL metadata',
            intent: Intent.DANGER,
          });
        }
        this.setState({
          columnMetadataState,
        });
      },
    });
  }

  componentDidMount(): void {
    this.metadataQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.metadataQueryManager.terminate();
  }

  private readonly handleWorkPanelClose = () => {
    this.setState({ showWorkHistory: false });
    localStorageSetJson(LocalStorageKeys.WORKBENCH_WORK_PANEL, false);
  };

  private readonly handleStats = (id: string) => {
    this.setState({
      queryStatsId: id,
    });
  };

  private getTabId(): string | undefined {
    const { tabId, initQuery } = this.props;
    if (tabId) return tabId;
    if (initQuery) return; // If initialized from a query go to the first tab, forget about the last opened tab
    return localStorageGet(LocalStorageKeys.WORKBENCH_LAST_TAB);
  }

  private getCurrentTabEntry() {
    const { tabEntries } = this.state;
    const tabId = this.getTabId();
    return tabEntries.find(({ id }) => id === tabId) || tabEntries[0];
  }

  private getCurrentQuery() {
    return this.getCurrentTabEntry().query;
  }

  private renderStatsDialog() {
    const { queryStatsId } = this.state;
    if (!queryStatsId) return;

    return (
      <ExecutionDetailsDialog
        id={queryStatsId}
        onClose={() => this.setState({ queryStatsId: undefined })}
      />
    );
  }

  private renderHistoryDialog() {
    const { historyDialogOpen } = this.state;
    if (!historyDialogOpen) return;

    return (
      <WorkbenchHistoryDialog
        onSelectQuery={query => this.handleNewTab(query, 'Query from history')}
        onClose={() => this.setState({ historyDialogOpen: false })}
      />
    );
  }

  private renderExternalConfigDialog() {
    const { initExternalConfig } = this.state;
    if (!initExternalConfig) return;

    return (
      <ExternalConfigDialog
        onSetExternalConfig={(externalConfig, isArrays) => {
          this.handleQueryChange(
            this.getCurrentQuery().insertExternalPanel(externalConfig, isArrays),
          );
        }}
        onClose={() => {
          this.setState({ initExternalConfig: false });
        }}
      />
    );
  }

  private renderTabRenameDialog() {
    const { renamingTab } = this.state;
    if (!renamingTab) return;

    return (
      <TabRenameDialog
        initialTabName={renamingTab.tabName}
        onSave={newTabName => {
          const { tabEntries } = this.state;
          if (!renamingTab) return;
          this.handleQueriesChange(
            tabEntries.map(tabEntry =>
              tabEntry.id === renamingTab.id ? { ...renamingTab, tabName: newTabName } : tabEntry,
            ),
          );
        }}
        onClose={() => this.setState({ renamingTab: undefined })}
      />
    );
  }

  private renderSpecDialog() {
    const { specDialogOpen } = this.state;
    if (!specDialogOpen) return;

    return (
      <SpecDialog
        onSubmit={spec => {
          let sql: string;
          try {
            sql = convertSpecToSql(spec as any);
          } catch (e) {
            AppToaster.show({
              message: `Could not convert spec: ${e.message}`,
              intent: Intent.DANGER,
            });
            return;
          }

          AppToaster.show({
            message: `Spec converted, please double check`,
            intent: Intent.SUCCESS,
          });
          this.handleNewTab(
            WorkbenchQuery.blank().changeQueryString(sql),
            'Convert ' + getSpecDatasourceName(spec as any),
          );
        }}
        onClose={() => this.setState({ specDialogOpen: false })}
        title="Ingestion spec to convert"
      />
    );
  }

  private renderToolbarMoreMenu() {
    const { extraEngines } = this.props;
    const query = this.getCurrentQuery();

    return (
      <Menu>
        <MenuItem
          icon={IconNames.DOCUMENT_SHARE}
          text="Extract helper queries"
          onClick={() => this.handleQueryChange(query.explodeQuery())}
        />
        <MenuItem
          icon={IconNames.DOCUMENT_OPEN}
          text="Materialize helper queries"
          onClick={() => this.handleQueryChange(query.materializeQuery())}
        />
        <MenuItem
          icon={IconNames.HISTORY}
          text="Query history"
          onClick={() => this.setState({ historyDialogOpen: true })}
        />
        {extraEngines.includes('sql-task') && (
          <MenuItem
            icon={IconNames.TEXT_HIGHLIGHT}
            text="Convert ingestion spec to SQL"
            onClick={() => this.setState({ specDialogOpen: true })}
          />
        )}
      </Menu>
    );
  }

  private renderToolbar() {
    const { extraEngines } = this.props;
    const { showWorkHistory } = this.state;

    return (
      <ButtonGroup className="toolbar">
        {extraEngines.includes('sql-task') && (
          <Button
            icon={IconNames.TH_DERIVED}
            text="Connect external data"
            onClick={e => {
              if (e.shiftKey && e.altKey) {
                this.handleQueriesChange(getDemoQueries());
              } else {
                this.setState({
                  initExternalConfig: true,
                });
              }
            }}
            minimal
          />
        )}
        <Popover2 content={this.renderToolbarMoreMenu()}>
          <Button icon={IconNames.WRENCH} minimal />
        </Popover2>
        {!showWorkHistory && (
          <Button
            icon={IconNames.ONE_COLUMN}
            minimal
            title="Show work history"
            onClick={() => {
              this.setState({ showWorkHistory: true });
              localStorageSetJson(LocalStorageKeys.WORKBENCH_WORK_PANEL, true);
            }}
          />
        )}
      </ButtonGroup>
    );
  }

  private renderCenterPanel() {
    const { onTabChange, mandatoryQueryContext, extraEngines } = this.props;
    const { columnMetadataState, tabEntries } = this.state;
    const currentTabEntry = this.getCurrentTabEntry();

    return (
      <div className="center-panel">
        <div className="query-tabs">
          {tabEntries.map((tabEntry, i) => {
            const currentId = tabEntry.id;
            const active = currentTabEntry === tabEntry;
            const disabled = tabEntries.length <= 1;
            return (
              <ButtonGroup key={i} minimal className={classNames('tab-button', { active })}>
                <Button
                  className="tab-name"
                  text={tabEntry.tabName}
                  title={tabEntry.tabName}
                  onClick={() => {
                    localStorageSet(LocalStorageKeys.WORKBENCH_LAST_TAB, currentId);
                    onTabChange(currentId);
                  }}
                  onDoubleClick={() => this.setState({ renamingTab: tabEntry })}
                />
                <Popover2
                  className="tab-extra"
                  position="bottom"
                  content={
                    <Menu>
                      <MenuItem
                        icon={IconNames.EDIT}
                        text="Rename tab"
                        onClick={() => this.setState({ renamingTab: tabEntry })}
                      />
                      <MenuItem
                        icon={IconNames.DUPLICATE}
                        text="Duplicate tab"
                        onClick={() => {
                          const id = generate8HexId();
                          const newTabEntry: TabEntry = {
                            id,
                            tabName: tabEntry.tabName + ' (copy)',
                            query: tabEntry.query.duplicate(),
                          };
                          this.handleQueriesChange(
                            tabEntries.slice(0, i + 1).concat(newTabEntry, tabEntries.slice(i + 1)),
                            () => {
                              onTabChange(newTabEntry.id);
                            },
                          );
                        }}
                      />
                      <MenuItem
                        icon={IconNames.CROSS}
                        text="Close tab"
                        intent={Intent.DANGER}
                        disabled={disabled}
                        onClick={() => {
                          cleanupTabEntry(tabEntry);
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id !== currentId),
                            () => {
                              if (!active) return;
                              onTabChange(tabEntries[Math.max(0, i - 1)].id);
                            },
                          );
                        }}
                      />
                      <MenuItem
                        icon={IconNames.CROSS}
                        text="Close other tabs"
                        intent={Intent.DANGER}
                        disabled={disabled}
                        onClick={() => {
                          tabEntries.forEach(tabEntry => {
                            if (tabEntry.id === currentId) return;
                            cleanupTabEntry(tabEntry);
                          });
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id === currentId),
                            () => {
                              if (!active) return;
                              onTabChange(tabEntry.id);
                            },
                          );
                        }}
                      />
                    </Menu>
                  }
                >
                  <Button icon={IconNames.MORE} />
                </Popover2>
              </ButtonGroup>
            );
          })}
          <Button
            className="add-tab"
            icon={IconNames.PLUS}
            minimal
            onClick={() => {
              this.handleNewTab(WorkbenchQuery.blank());
            }}
          />
        </div>
        {this.renderToolbar()}
        <QueryTab
          key={currentTabEntry.id}
          query={currentTabEntry.query}
          mandatoryQueryContext={mandatoryQueryContext}
          columnMetadata={columnMetadataState.getSomeData()}
          onQueryChange={this.handleQueryChange}
          onStats={this.handleStats}
          extraEngines={extraEngines}
        />
      </div>
    );
  }

  private readonly handleQueriesChange = (newQueries: TabEntry[], callback?: () => void) => {
    localStorageSetJson(LocalStorageKeys.WORKBENCH_QUERIES, newQueries);
    this.setState({ tabEntries: newQueries }, callback);
  };

  private readonly handleQueryChange = (newQuery: WorkbenchQuery, _preferablyRun?: boolean) => {
    const { tabEntries } = this.state;
    const tabId = this.getTabId();
    const tabIndex = Math.max(
      tabEntries.findIndex(({ id }) => id === tabId),
      0,
    );
    const newQueries = deepSet(tabEntries, `${tabIndex}.query`, newQuery);
    this.handleQueriesChange(newQueries); // preferablyRun ? this.handleRunIfLive : undefined
  };

  private readonly handleQueryStringChange = (
    queryString: string,
    preferablyRun?: boolean,
  ): void => {
    this.handleQueryChange(this.getCurrentQuery().changeQueryString(queryString), preferablyRun);
  };

  private readonly handleSqlQueryChange = (sqlQuery: SqlQuery, preferablyRun?: boolean): void => {
    this.handleQueryStringChange(sqlQuery.toString(), preferablyRun);
  };

  private readonly getParsedQuery = () => {
    return this.getCurrentQuery().getParsedQuery();
  };

  private readonly handleNewTab = (query: WorkbenchQuery, tabName?: string) => {
    const { onTabChange } = this.props;
    const { tabEntries } = this.state;
    const id = generate8HexId();
    const newTabEntry: TabEntry = {
      id,
      tabName: tabName || `Tab ${tabEntries.length + 1}`,
      query,
    };
    this.handleQueriesChange(tabEntries.concat(newTabEntry), () => {
      onTabChange(newTabEntry.id);
    });
  };

  render(): JSX.Element {
    const { columnMetadataState, showWorkHistory } = this.state;
    const query = this.getCurrentQuery();

    let defaultSchema;
    let defaultTable;
    const parsedQuery = query.getParsedQuery();
    if (parsedQuery) {
      defaultSchema = parsedQuery.getFirstSchema();
      defaultTable = parsedQuery.getFirstTableName();
    }

    return (
      <div
        className={classNames('workbench-view app-view', {
          'hide-column-tree': columnMetadataState.isError(),
          'hide-work-history': !showWorkHistory,
        })}
      >
        {!columnMetadataState.isError() && (
          <ColumnTree
            getParsedQuery={this.getParsedQuery}
            columnMetadataLoading={columnMetadataState.loading}
            columnMetadata={columnMetadataState.data}
            onQueryChange={this.handleSqlQueryChange}
            defaultSchema={defaultSchema ? defaultSchema : 'druid'}
            defaultTable={defaultTable}
            highlightTable={undefined}
          />
        )}
        {this.renderCenterPanel()}
        {showWorkHistory && (
          <WorkPanel
            onClose={this.handleWorkPanelClose}
            onExecutionDetails={this.handleStats}
            onRunQuery={query => this.handleQueryStringChange(query, true)}
            onNewTab={this.handleNewTab}
          />
        )}
        {this.renderStatsDialog()}
        {this.renderHistoryDialog()}
        {this.renderExternalConfigDialog()}
        {this.renderTabRenameDialog()}
        {this.renderSpecDialog()}
        <MetadataChangeDetector onChange={() => this.metadataQueryManager.runQuery(null)} />
      </div>
    );
  }
}
