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

import { AnchorButton, Button, ButtonGroup, Intent, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { SqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { MenuCheckbox } from '../../components';
import { AppToaster } from '../../singletons';
import { generate6HexId, TalariaQuery } from '../../talaria-models';
import {
  ColumnMetadata,
  deepSet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSetJson,
  queryDruidSql,
  QueryManager,
  QueryState,
  QueryWithContext,
} from '../../utils';
import { ColumnTree } from '../query-view/column-tree/column-tree';
import { ExplainDialog } from '../query-view/explain-dialog/explain-dialog';
import {
  LIVE_QUERY_MODES,
  LiveQueryMode,
} from '../query-view/live-query-mode-selector/live-query-mode-selector';

import { getDemoQueries } from './demo-queries';
import { ExternalConfigDialog } from './external-config-dialog/external-config-dialog';
import { MetadataChangeDetector } from './metadata-change-detector';
import { QueryTab } from './query-tab/query-tab';
import { TabRenameDialog } from './tab-rename-dialog/tab-rename-dialog';
import { TalariaHistoryDialog } from './talaria-history-dialog/talaria-history-dialog';
import { TalariaStatsDialog } from './talaria-stats-dialog/talaria-stats-dialog';
import { TalariaTabCache } from './talaria-tab-cache';
import { TabEntry } from './talaria-utils';
import { WorkPanel } from './work-panel/work-panel';

import './talaria-view.scss';

export interface TalariaViewProps {
  tabId: string | undefined;
  initQuery: string | undefined;
  defaultQueryContext?: Record<string, any>;
  mandatoryQueryContext?: Record<string, any>;
}

export interface TalariaViewState {
  tabEntries: TabEntry[];
  liveQueryMode: LiveQueryMode;

  columnMetadataState: QueryState<readonly ColumnMetadata[]>;

  explainDialogQuery?: QueryWithContext;

  initExternalConfig: boolean;
  talariaStatsTaskId?: string;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  renamingTab?: TabEntry;

  showWorkHistory: boolean;
}

export class TalariaView extends React.PureComponent<TalariaViewProps, TalariaViewState> {
  private readonly metadataQueryManager: QueryManager<null, ColumnMetadata[]>;

  constructor(props: TalariaViewProps, context: any) {
    super(props, context);

    const possibleTabEntries: TabEntry[] = localStorageGetJson(LocalStorageKeys.TALARIA_QUERIES);
    const possibleLiveQueryMode = localStorageGetJson(LocalStorageKeys.TALARIA_LIVE_MODE);
    const liveQueryMode = LIVE_QUERY_MODES.includes(possibleLiveQueryMode)
      ? possibleLiveQueryMode
      : 'auto';

    const showWorkHistory = Boolean(localStorageGetJson(LocalStorageKeys.TALARIA_WORK_HISTORY));

    this.state = {
      tabEntries:
        Array.isArray(possibleTabEntries) && possibleTabEntries.length
          ? possibleTabEntries.map(q => ({ ...q, query: new TalariaQuery(q.query) }))
          : [
              {
                id: generate6HexId(),
                tabName: 'First tab',
                query: TalariaQuery.blank().changeQueryContext(props.defaultQueryContext || {}),
              },
            ],
      liveQueryMode,

      columnMetadataState: QueryState.INIT,

      editContextDialogOpen: false,
      historyDialogOpen: false,
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

  private readonly handleStats = (taskId: string) => {
    this.setState({
      talariaStatsTaskId: taskId,
    });
  };

  private getCurrentTabEntry() {
    const { tabId } = this.props;
    const { tabEntries } = this.state;
    return tabEntries.find(({ id }) => id === tabId) || tabEntries[0];
  }

  private getCurrentQuery() {
    return this.getCurrentTabEntry().query;
  }

  private readonly handleStatsFromId = (taskId: string) => {
    this.setState({ talariaStatsTaskId: taskId });
  };

  private renderStatsDialog() {
    const { talariaStatsTaskId } = this.state;
    if (!talariaStatsTaskId) return;

    return (
      <TalariaStatsDialog
        taskId={talariaStatsTaskId}
        onClose={() => this.setState({ talariaStatsTaskId: undefined })}
      />
    );
  }

  private renderExplainDialog() {
    const { mandatoryQueryContext } = this.props;
    const { explainDialogQuery } = this.state;
    if (!explainDialogQuery) return;

    return (
      <ExplainDialog
        queryWithContext={explainDialogQuery}
        mandatoryQueryContext={mandatoryQueryContext}
        setQueryString={this.handleQueryStringChange}
        onClose={() => this.setState({ explainDialogQuery: undefined })}
      />
    );
  }

  private renderHistoryDialog() {
    const { historyDialogOpen } = this.state;
    if (!historyDialogOpen) return;

    return (
      <TalariaHistoryDialog
        setQueryString={this.handleQueryChange}
        onClose={() => this.setState({ historyDialogOpen: false })}
      />
    );
  }

  private renderExternalConfigDialog() {
    const { initExternalConfig } = this.state;
    if (!initExternalConfig) return;

    return (
      <ExternalConfigDialog
        onSetExternalConfig={externalConfig => {
          this.handleQueryChange(this.getCurrentQuery().insertExternalPanel(externalConfig));
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
        tabName={renamingTab.tabName}
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

  private renderToolbarMoreMenu() {
    const { showWorkHistory } = this.state;
    const query = this.getCurrentQuery();

    return (
      <Menu>
        <MenuItem
          icon={IconNames.DOCUMENT_SHARE}
          text="Explode query"
          onClick={() => this.handleQueryChange(query.explodeQuery())}
        />
        <MenuItem
          icon={IconNames.DOCUMENT_OPEN}
          text="Materialize query"
          onClick={() => this.handleQueryChange(query.materializeQuery())}
        />
        <MenuItem
          icon={IconNames.HISTORY}
          text="Query history"
          onClick={() => this.setState({ historyDialogOpen: true })}
        />
        <MenuCheckbox
          checked={showWorkHistory}
          text="Show work history"
          onChange={() => {
            this.setState({ showWorkHistory: !showWorkHistory });
            localStorageSetJson(LocalStorageKeys.TALARIA_WORK_HISTORY, !showWorkHistory);
          }}
        />
      </Menu>
    );
  }

  private renderToolbar() {
    return (
      <div className="toolbar">
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
        <Popover2 content={this.renderToolbarMoreMenu()}>
          <Button icon={IconNames.WRENCH} minimal />
        </Popover2>
      </div>
    );
  }

  private renderCenterPanel() {
    const { mandatoryQueryContext } = this.props;
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
                <AnchorButton text={tabEntry.tabName} href={`#talaria/${currentId}`} />
                <Popover2
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
                          const id = generate6HexId();
                          const newTabEntry: TabEntry = {
                            id,
                            tabName: `Tab ${id.substr(0, 4)}`,
                            query: tabEntry.query,
                          };
                          this.handleQueriesChange(
                            tabEntries.slice(0, i + 1).concat(newTabEntry, tabEntries.slice(i + 1)),
                            () => {
                              location.hash = `#talaria/${newTabEntry.id}`;
                            },
                          );
                        }}
                      />
                      <MenuItem
                        icon={IconNames.TRASH}
                        text="Close tab"
                        intent={Intent.DANGER}
                        disabled={disabled}
                        onClick={() => {
                          TalariaTabCache.deleteStates(tabEntry.query.getIds());
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id !== currentId),
                            () => {
                              if (!active) return;
                              location.hash = `#talaria/${tabEntries[i - 1].id}`;
                            },
                          );
                        }}
                      />
                      <MenuItem
                        icon={IconNames.TRASH}
                        text="Close other tabs"
                        intent={Intent.DANGER}
                        disabled={disabled}
                        onClick={() => {
                          tabEntries.forEach(tabEntry => {
                            if (tabEntry.id === currentId) return;
                            TalariaTabCache.deleteStates(tabEntry.query.getIds());
                          });
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id === currentId),
                            () => {
                              if (!active) return;
                              location.hash = `#talaria/${tabEntry.id}`;
                            },
                          );
                        }}
                      />
                    </Menu>
                  }
                >
                  <Button icon={IconNames.CARET_DOWN} />
                </Popover2>
              </ButtonGroup>
            );
          })}
          <Button
            className="add-tab"
            icon={IconNames.PLUS}
            minimal
            onClick={() => {
              const id = generate6HexId();
              const newTabEntry: TabEntry = {
                id,
                tabName: `Tab ${id.substr(0, 4)}`,
                query: TalariaQuery.blank(),
              };
              this.handleQueriesChange(tabEntries.concat(newTabEntry), () => {
                location.hash = `#talaria/${newTabEntry.id}`;
              });
            }}
          />
          {this.renderToolbar()}
        </div>
        <QueryTab
          key={currentTabEntry.id}
          query={currentTabEntry.query}
          mandatoryQueryContext={mandatoryQueryContext}
          columnMetadata={columnMetadataState.getSomeData()}
          onQueryChange={this.handleQueryChange}
          onStats={this.handleStats}
        />
      </div>
    );
  }

  private readonly handleQueriesChange = (newQueries: TabEntry[], callback?: () => void) => {
    localStorageSetJson(LocalStorageKeys.TALARIA_QUERIES, newQueries);
    this.setState({ tabEntries: newQueries }, callback);
  };

  private readonly handleQueryChange = (newQuery: TalariaQuery, _preferablyRun?: boolean) => {
    const { tabId } = this.props;
    const { tabEntries } = this.state;
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
        className={classNames('talaria-view app-view', {
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
            onStats={this.handleStatsFromId}
            onRunQuery={query => this.handleQueryStringChange(query, true)}
          />
        )}
        {this.renderStatsDialog()}
        {this.renderExplainDialog()}
        {this.renderHistoryDialog()}
        {this.renderExternalConfigDialog()}
        {this.renderTabRenameDialog()}
        <MetadataChangeDetector onChange={() => this.metadataQueryManager.runQuery(null)} />
      </div>
    );
  }
}
