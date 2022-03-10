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

import {
  AnchorButton,
  Button,
  ButtonGroup,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { SqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { MenuCheckbox } from '../../components';
import { SpecDialog } from '../../dialogs';
import { getLink } from '../../links';
import { AppToaster } from '../../singletons';
import { AceEditorStateCache } from '../../singletons/ace-editor-state-cache';
import { generate8HexId, TabEntry, TalariaQuery } from '../../talaria-models';
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
import { ColumnTree } from '../query-view/column-tree/column-tree';
import {
  LIVE_QUERY_MODES,
  LiveQueryMode,
} from '../query-view/live-query-mode-selector/live-query-mode-selector';

import { getDemoQueries } from './demo-queries';
import { ExternalConfigDialog } from './external-config-dialog/external-config-dialog';
import { MetadataChangeDetector } from './metadata-change-detector';
import { QueryTab } from './query-tab/query-tab';
import { convertSpecToSql } from './spec-conversion';
import { TabRenameDialog } from './tab-rename-dialog/tab-rename-dialog';
import { TalariaHistoryDialog } from './talaria-history-dialog/talaria-history-dialog';
import { TalariaQueryStateCache } from './talaria-query-state-cache';
import { TalariaStatsDialog } from './talaria-stats-dialog/talaria-stats-dialog';
import { WorkPanel } from './work-panel/work-panel';

import './talaria-view.scss';

function cleanupTabEntry(tabEntry: TabEntry): void {
  const discardedIds = tabEntry.query.getIds();
  TalariaQueryStateCache.deleteStates(discardedIds);
  AceEditorStateCache.deleteStates(discardedIds);
}

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

  initExternalConfig: boolean;
  talariaStatsTaskId?: string;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  specDialogOpen: boolean;
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

    const tabEntries =
      Array.isArray(possibleTabEntries) && possibleTabEntries.length
        ? possibleTabEntries.map(q => ({ ...q, query: new TalariaQuery(q.query) }))
        : [];

    const { initQuery } = props;
    if (initQuery) {
      // Put it in the front so that it is the opened tab
      tabEntries.unshift({
        id: generate8HexId(),
        tabName: 'Opened query',
        query: TalariaQuery.blank()
          .changeQueryString(initQuery)
          .changeQueryContext(props.defaultQueryContext || {}),
      });
    }

    if (!tabEntries.length) {
      tabEntries.push({
        id: generate8HexId(),
        tabName: 'Tab 1',
        query: TalariaQuery.blank().changeQueryContext(props.defaultQueryContext || {}),
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

  private readonly handleStats = (taskId: string) => {
    this.setState({
      talariaStatsTaskId: taskId,
    });
  };

  private getTabId(): string | undefined {
    const { tabId, initQuery } = this.props;
    if (tabId) return tabId;
    if (initQuery) return; // If initialized from a query go to the first tab, forget about the last opened tab
    return localStorageGet(LocalStorageKeys.TALARIA_LAST_TAB);
  }

  private getCurrentTabEntry() {
    const { tabEntries } = this.state;
    const tabId = this.getTabId();
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
          this.handleQueryStringChange(sql);
        }}
        onClose={() => this.setState({ specDialogOpen: false })}
        title="Ingestion spec to convert"
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
        <MenuItem
          icon={IconNames.TEXT_HIGHLIGHT}
          text="Convert ingestion spec to SQL"
          onClick={() => this.setState({ specDialogOpen: true })}
        />
        <MenuCheckbox
          checked={showWorkHistory}
          text="Show work history"
          onChange={() => {
            this.setState({ showWorkHistory: !showWorkHistory });
            localStorageSetJson(LocalStorageKeys.TALARIA_WORK_HISTORY, !showWorkHistory);
          }}
        />
        <MenuDivider />
        <MenuItem
          icon={IconNames.HELP}
          text="DruidSQL documentation"
          href={getLink('DOCS_SQL')}
          target="_blank"
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
                <AnchorButton
                  className="tab-name"
                  text={tabEntry.tabName}
                  title={tabEntry.tabName}
                  href={`#query-next/${currentId}`}
                  onClick={() => {
                    localStorageSet(LocalStorageKeys.TALARIA_LAST_TAB, currentId);
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
                              location.hash = `#query-next/${newTabEntry.id}`;
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
                          cleanupTabEntry(tabEntry);
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id !== currentId),
                            () => {
                              if (!active) return;
                              location.hash = `#query-next/${tabEntries[Math.max(0, i - 1)].id}`;
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
                            cleanupTabEntry(tabEntry);
                          });
                          this.handleQueriesChange(
                            tabEntries.filter(({ id }) => id === currentId),
                            () => {
                              if (!active) return;
                              location.hash = `#query-next/${tabEntry.id}`;
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
              this.handleNewTab(TalariaQuery.blank());
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
        />
      </div>
    );
  }

  private readonly handleQueriesChange = (newQueries: TabEntry[], callback?: () => void) => {
    localStorageSetJson(LocalStorageKeys.TALARIA_QUERIES, newQueries);
    this.setState({ tabEntries: newQueries }, callback);
  };

  private readonly handleQueryChange = (newQuery: TalariaQuery, _preferablyRun?: boolean) => {
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

  private readonly handleNewTab = (talariaQuery: TalariaQuery, tabName?: string) => {
    const { tabEntries } = this.state;
    const id = generate8HexId();
    const newTabEntry: TabEntry = {
      id,
      tabName: tabName || `Tab ${tabEntries.length + 1}`,
      query: talariaQuery,
    };
    this.handleQueriesChange(tabEntries.concat(newTabEntry), () => {
      location.hash = `#query-next/${newTabEntry.id}`;
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
