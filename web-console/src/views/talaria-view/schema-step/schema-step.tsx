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
  Button,
  ButtonGroup,
  Callout,
  ControlGroup,
  InputGroup,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { QueryResult, SqlExpression, SqlFunction, SqlQuery, SqlRef } from 'druid-query-toolkit';
import React, { useCallback, useMemo, useState } from 'react';

import { Loader } from '../../../components';
import { AsyncActionDialog } from '../../../dialogs';
import { possibleDruidFormatForValues, TIME_COLUMN } from '../../../druid-models';
import { useLastDefined, usePermanentCallback, useQueryManager } from '../../../hooks';
import { getLink } from '../../../links';
import {
  ExternalConfig,
  fitIngestQueryPattern,
  IngestQueryPattern,
  ingestQueryPatternToQuery,
  QueryExecution,
  summarizeExternalConfig,
} from '../../../talaria-models';
import {
  caseInsensitiveContains,
  change,
  filterMap,
  getContextFromSqlQuery,
  oneOf,
  QueryAction,
  wait,
  without,
} from '../../../utils';
import { dataTypeToIcon } from '../../query-view/query-utils';
import {
  extractQueryResults,
  submitAsyncQuery,
  talariaBackgroundResultStatusCheck,
} from '../execution-utils';
import { ExpressionEditorDialog } from '../expression-editor-dialog/expression-editor-dialog';
import { ExternalConfigDialog } from '../external-config-dialog/external-config-dialog';
import { timeFormatToSql } from '../sql-utils';
import { TalariaQueryInput } from '../talaria-query-input/talaria-query-input';

import { ColumnList } from './column-list/column-list';
import { PreviewTable } from './preview-table/preview-table';
import { RollupAnalysisPane } from './rollup-analysis-pane/rollup-analysis-pane';

import './schema-step.scss';

export function changeByString<T>(xs: readonly T[], from: T, to: T): T[] {
  const fromString = String(from);
  return xs.map(x => (String(x) === fromString ? to : x));
}

function digestQueryString(queryString: string): {
  ingestQueryPattern?: IngestQueryPattern;
  ingestPatternError?: string;
  parsedQuery?: SqlQuery;
} {
  let ingestQueryPattern: IngestQueryPattern | undefined;
  const parsedQuery = SqlQuery.maybeParse(queryString);
  let ingestPatternError: string | undefined;
  if (parsedQuery) {
    try {
      ingestQueryPattern = fitIngestQueryPattern(parsedQuery);
    } catch (e) {
      ingestPatternError = e.message;
    }
  } else {
    ingestPatternError = 'Unparsable query';
  }

  return {
    ingestQueryPattern,
    ingestPatternError,
    parsedQuery,
  };
}

interface TimeSuggestion {
  label: string | JSX.Element;
  queryAction: QueryAction;
}

function getTimeSuggestions(queryResult: QueryResult, parsedQuery: SqlQuery): TimeSuggestion[] {
  const timeColumnIndex = queryResult.header.findIndex(({ name }) => name === TIME_COLUMN);
  if (timeColumnIndex !== -1) {
    const timeColumn = queryResult.header[timeColumnIndex];
    switch (timeColumn.sqlType) {
      case 'TIMESTAMP':
        return []; // All good, nothing to do

      case 'VARCHAR':
      case 'BIGINT': {
        const selectExpression = parsedQuery.getSelectExpressionForIndex(timeColumnIndex);
        if (!selectExpression) return [];

        const values = queryResult.rows.map(row => row[timeColumnIndex]);
        const possibleDruidFormat = possibleDruidFormatForValues(values);
        const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;
        if (!formatSql) return [];
        const newSelectExpression = formatSql.fillPlaceholders([
          selectExpression.getUnderlyingExpression(),
        ]);

        return [
          {
            label: `Parse as '${possibleDruidFormat}'`,
            queryAction: q =>
              q
                .removeSelectIndex(timeColumnIndex)
                .addSelect(newSelectExpression.as(TIME_COLUMN), { insertIndex: 0 }),
          },
        ];
      }

      default:
        return []; // ToDo: Make some suggestion here
    }
  } else {
    return filterMap(queryResult.header, (c, i) => {
      const selectExpression = parsedQuery.getSelectExpressionForIndex(i);
      if (!selectExpression) return;

      if (c.sqlType === 'TIMESTAMP') {
        return {
          label: (
            <>
              {'Use '}
              <Tag minimal round>
                {c.name}
              </Tag>
              {' as the primary time column'}
            </>
          ),
          queryAction: q =>
            q
              .removeSelectIndex(timeColumnIndex)
              .addSelect(selectExpression.as(TIME_COLUMN), { insertIndex: 0 }),
        };
      }

      const values = queryResult.rows.map(row => row[i]);
      const possibleDruidFormat = possibleDruidFormatForValues(values);
      const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;
      if (!formatSql) return;
      const newSelectExpression = formatSql.fillPlaceholders([
        selectExpression.getUnderlyingExpression(),
      ]);

      return {
        label: (
          <>
            {`Use `}
            <Tag minimal round>
              {c.name}
            </Tag>
            {` parsed as '${possibleDruidFormat}'`}
          </>
        ),
        queryAction: q =>
          q.removeSelectIndex(i).addSelect(newSelectExpression.as(TIME_COLUMN), { insertIndex: 0 }),
      };
    });
  }
}

const GRANULARITIES: string[] = ['hour', 'day', 'month', 'all'];

type Mode = 'table' | 'list' | 'sql';

export interface SchemaStepProps {
  queryString: string;
  onQueryStringChange(queryString: string): void;
  goToQuery: () => void;
  onBack(): void;
  onDone(): void;
}

export const SchemaStep = function SchemaStep(props: SchemaStepProps) {
  const { queryString, onQueryStringChange, goToQuery, onBack, onDone } = props;
  const [mode, setMode] = useState<Mode>('table');
  const [columnSearch, setColumnSearch] = useState('');
  const [showAddExternal, setShowAddExternal] = useState(false);
  const [externalInEditor, setExternalInEditor] = useState<ExternalConfig | undefined>();
  const [addColumnType, setAddColumnType] = useState<'dimension' | 'metric' | undefined>();
  const [columnInEditor, setColumnInEditor] = useState<SqlExpression | undefined>();
  const [showAddFilterEditor, setShowAddFilterEditor] = useState(false);
  const [filterInEditor, setFilterInEditor] = useState<SqlExpression | undefined>();
  const [showRollupConfirm, setShowRollupConfirm] = useState(false);
  const [showRollupAnalysisPane, setShowRollupAnalysisPane] = useState(false);

  const columnFilter = useCallback(
    (columnName: string) => caseInsensitiveContains(columnName, columnSearch),
    [columnSearch],
  );

  const { ingestQueryPattern, ingestPatternError, parsedQuery } = useMemo(
    () => digestQueryString(queryString),
    [queryString],
  );

  const updatePattern = useCallback(
    (ingestQueryPattern: IngestQueryPattern) => {
      onQueryStringChange(ingestQueryPatternToQuery(ingestQueryPattern).toString());
    },
    [onQueryStringChange],
  );

  const effectiveMode: Mode = ingestQueryPattern ? mode : 'sql';

  const handleQueryAction = usePermanentCallback((queryAction: QueryAction) => {
    if (!parsedQuery) return;
    onQueryStringChange(parsedQuery.apply(queryAction).toString());
  });

  function toggleRollup() {
    if (!ingestQueryPattern) return;

    if (ingestQueryPattern.metrics) {
      updatePattern({ ...ingestQueryPattern, metrics: undefined });
    } else {
      const countExpression = ingestQueryPattern.dimensions.find(groupedExpression =>
        oneOf(groupedExpression.getOutputName(), 'count', '__count'),
      );

      updatePattern({
        ...ingestQueryPattern,
        dimensions: without(ingestQueryPattern.dimensions, countExpression),
        metrics: [
          (countExpression
            ? SqlFunction.simple('SUM', [countExpression.getUnderlyingExpression()])
            : SqlFunction.COUNT_STAR
          ).as(countExpression?.getOutputName() || 'count'),
        ],
      });
    }
  }

  const previewQueryString = useLastDefined(
    ingestQueryPattern && mode !== 'sql'
      ? ingestQueryPatternToQuery(ingestQueryPattern, true).toString()
      : undefined,
  );

  const [previewResultState] = useQueryManager<string, QueryResult, QueryExecution>({
    query: previewQueryString,
    processQuery: async (previewQueryString: string, cancelToken) => {
      return extractQueryResults(
        await submitAsyncQuery({
          query: previewQueryString,
          context: {
            ...getContextFromSqlQuery(previewQueryString),
            talaria: true,
            sqlOuterLimit: 50,
          },
          cancelToken,
        }),
      );
    },
    backgroundStatusCheck: talariaBackgroundResultStatusCheck,
  });

  const unusedColumns = ingestQueryPattern
    ? ingestQueryPattern.mainExternalConfig.columns.filter(
        ({ name }) =>
          !ingestQueryPattern.dimensions.some(d => d.containsColumn(name)) &&
          !ingestQueryPattern.metrics?.some(m => m.containsColumn(name)),
      )
    : [];

  const segmentGranularity = ingestQueryPattern?.segmentGranularity;

  const timeSuggestions =
    previewResultState.data && parsedQuery
      ? getTimeSuggestions(previewResultState.data, parsedQuery)
      : [];

  const previewResultSomeData = previewResultState.getSomeData();

  return (
    <div className={classNames('schema-step', { 'with-analysis': showRollupAnalysisPane })}>
      <div className="loader-controls">
        <div className="control-line">
          <Button icon={IconNames.ARROW_LEFT} minimal onClick={onBack} />
          <Popover2
            position="bottom"
            content={
              <Menu>
                {ingestQueryPattern && (
                  <>
                    <MenuItem
                      icon={IconNames.DATABASE}
                      text={summarizeExternalConfig(ingestQueryPattern.mainExternalConfig)}
                      onClick={() => setExternalInEditor(ingestQueryPattern.mainExternalConfig)}
                    />
                    <MenuDivider />
                  </>
                )}
                <MenuItem
                  icon={IconNames.PLUS}
                  text="Add source"
                  onClick={() => setShowAddExternal(true)}
                />
              </Menu>
            }
          >
            <Button icon={IconNames.DATABASE} minimal>
              Sources &nbsp;
              <Tag minimal round>
                1
              </Tag>
            </Button>
          </Popover2>
          <Popover2
            position="bottom"
            content={
              <Menu>
                <MenuItem
                  icon={IconNames.PLUS}
                  text="Add filter"
                  onClick={() => setShowAddFilterEditor(true)}
                />
                {ingestQueryPattern && ingestQueryPattern.filters.length > 0 && (
                  <>
                    <MenuDivider />
                    {ingestQueryPattern.filters.map((filter, i) => (
                      <MenuItem
                        key={i}
                        icon={IconNames.FILTER}
                        text={filter.toString()}
                        onClick={() => setFilterInEditor(filter)}
                      />
                    ))}
                  </>
                )}
              </Menu>
            }
          >
            <Button icon={IconNames.FILTER} minimal>
              Filters &nbsp;
              <Tag minimal round>
                {ingestQueryPattern ? ingestQueryPattern.filters.length : '?'}
              </Tag>
            </Button>
          </Popover2>
          <Button icon={IconNames.COMPRESSED} onClick={() => setShowRollupConfirm(true)} minimal>
            Rollup &nbsp;
            <Tag minimal round>
              {ingestQueryPattern?.metrics ? 'On' : 'Off'}
            </Tag>
          </Button>
          {ingestQueryPattern?.metrics && (
            <Button
              icon={IconNames.LIGHTBULB}
              text="Analyze rollup"
              minimal
              active={showRollupAnalysisPane}
              onClick={() => setShowRollupAnalysisPane(!showRollupAnalysisPane)}
            />
          )}
        </div>
        <ControlGroup className="right-controls">
          {ingestQueryPattern && (
            <Popover2
              position="bottom"
              content={
                <Menu>
                  <MenuDivider title="Primary partitioning (by time)" />
                  {segmentGranularity ? (
                    <MenuItem
                      icon={IconNames.TIME}
                      text={
                        <>
                          {'Segment granularity '}
                          <Tag minimal round>
                            {segmentGranularity}
                          </Tag>
                        </>
                      }
                    >
                      {GRANULARITIES.map(g => (
                        <MenuItem
                          key={g}
                          icon={g === segmentGranularity ? IconNames.TICK : IconNames.BLANK}
                          text={g}
                          onClick={() => {
                            if (!ingestQueryPattern) return;
                            updatePattern({
                              ...ingestQueryPattern,
                              segmentGranularity: g,
                            });
                          }}
                        />
                      ))}
                    </MenuItem>
                  ) : (
                    <MenuItem
                      text="Only available when a primary time column is selected"
                      disabled
                    />
                  )}
                  <MenuDivider title="Secondary partitioning" />
                  {ingestQueryPattern.partitions.map((p, i) => (
                    <MenuItem
                      key={i}
                      icon={IconNames.SPLIT_COLUMNS}
                      text={ingestQueryPattern.dimensions[p].getOutputName()}
                      onClick={() =>
                        updatePattern({
                          ...ingestQueryPattern,
                          partitions: without(ingestQueryPattern.partitions, p),
                        })
                      }
                    />
                  ))}
                  <MenuItem icon={IconNames.PLUS} text="Add column partitioning">
                    {filterMap(ingestQueryPattern.dimensions, (dimension, i) => {
                      const outputName = dimension.getOutputName();
                      if (outputName === TIME_COLUMN || ingestQueryPattern.partitions.includes(i)) {
                        return;
                      }

                      return (
                        <MenuItem
                          key={i}
                          text={outputName}
                          onClick={() =>
                            updatePattern({
                              ...ingestQueryPattern,
                              partitions: ingestQueryPattern.partitions.concat([i]),
                            })
                          }
                        />
                      );
                    })}
                  </MenuItem>
                  <MenuDivider />
                  <MenuItem
                    icon={IconNames.HELP}
                    text="Learn more about partitioning"
                    href={`${getLink('DOCS')}/ingestion/partitioning.html`}
                    target="_blank"
                  />
                </Menu>
              }
            >
              <Button minimal>
                Partitioning &nbsp;
                <Tag minimal round>
                  {(segmentGranularity && segmentGranularity !== 'all' ? 1 : 0) +
                    ingestQueryPattern.partitions.length}
                </Tag>
              </Button>
            </Popover2>
          )}
          <Button minimal>
            Destination &nbsp;
            <Tag minimal round>
              {ingestQueryPattern?.insertTableName || ''}
            </Tag>
          </Button>
          <Button
            icon={IconNames.CLOUD_UPLOAD}
            text="Start loading data"
            intent={Intent.PRIMARY}
            onClick={onDone}
          />
        </ControlGroup>
        <div className="control-line bottom-left">
          <ButtonGroup>
            <Button
              icon={IconNames.TH_LIST}
              text="Table"
              disabled={!ingestQueryPattern}
              active={effectiveMode === 'table'}
              onClick={() => setMode('table')}
            />
            <Button
              icon={IconNames.LIST_COLUMNS}
              text="List"
              disabled={!ingestQueryPattern}
              active={effectiveMode === 'list'}
              onClick={() => setMode('list')}
            />
            <Button
              icon={IconNames.APPLICATION}
              text="SQL"
              active={effectiveMode === 'sql'}
              onClick={() => setMode('sql')}
            />
          </ButtonGroup>
          {timeSuggestions.length > 0 && (
            <Popover2
              content={
                <Menu>
                  {timeSuggestions.map((timeSuggestion, i) => (
                    <MenuItem
                      key={i}
                      icon={IconNames.CLEAN}
                      text={timeSuggestion.label}
                      onClick={() => handleQueryAction(timeSuggestion.queryAction)}
                    />
                  ))}
                  <MenuItem
                    icon={IconNames.HELP}
                    text="Learn more about the primary time column"
                    href={`${getLink('DOCS')}/ingestion/data-model.html#primary-timestamp`}
                    target="_blank"
                  />
                </Menu>
              }
            >
              <Button
                className="time-column-warning"
                icon={IconNames.WARNING_SIGN}
                text="No primary time column selected"
                intent={Intent.WARNING}
                minimal
              />
            </Popover2>
          )}
        </div>
        {effectiveMode !== 'sql' && ingestQueryPattern && (
          <div className="control-line bottom-right">
            <Popover2
              className="add-column-control"
              position="bottom"
              content={
                <Menu>
                  {ingestQueryPattern.metrics ? (
                    <>
                      <MenuItem
                        icon={IconNames.PLUS}
                        text="Custom dimension"
                        onClick={() => setAddColumnType('dimension')}
                      />
                      <MenuItem
                        icon={IconNames.PLUS}
                        text="Custom metric"
                        onClick={() => setAddColumnType('metric')}
                      />
                    </>
                  ) : (
                    <MenuItem
                      icon={IconNames.PLUS}
                      text="Custom column"
                      onClick={() => setAddColumnType('dimension')}
                    />
                  )}
                  <MenuDivider />
                  {unusedColumns.length ? (
                    unusedColumns.map((column, i) => (
                      <MenuItem
                        key={i}
                        icon={dataTypeToIcon(column.type)}
                        text={column.name}
                        onClick={() => {
                          handleQueryAction(q =>
                            q.addSelect(
                              SqlRef.column(column.name),
                              ingestQueryPattern.metrics
                                ? { insertIndex: 'last-grouping', addToGroupBy: 'end' }
                                : {},
                            ),
                          );
                        }}
                      />
                    ))
                  ) : (
                    <MenuItem icon={IconNames.BLANK} text="No column suggestions" disabled />
                  )}
                </Menu>
              }
            >
              <Button icon={IconNames.PLUS} text="Add column" minimal />
            </Popover2>
            <InputGroup
              className="column-filter-control"
              value={columnSearch}
              placeholder="Search columns"
              onChange={e => {
                setColumnSearch(e.target.value);
              }}
            />
          </div>
        )}
        {effectiveMode === 'sql' && (
          <div className="control-line bottom-right">
            <Button rightIcon={IconNames.ARROW_TOP_RIGHT} onClick={goToQuery}>
              Open in <strong>Query</strong> view
            </Button>
          </div>
        )}
      </div>
      <div className="preview">
        {effectiveMode === 'table' && (
          <>
            {previewResultState.isError() ? (
              <div>{previewResultState.getErrorMessage()}</div>
            ) : (
              previewResultSomeData && (
                <PreviewTable
                  queryResult={previewResultSomeData}
                  onQueryAction={handleQueryAction}
                  columnFilter={columnFilter}
                  onEditColumn={setColumnInEditor}
                />
              )
            )}
            {previewResultState.isLoading() && <Loader />}
          </>
        )}
        {effectiveMode === 'list' &&
          ingestQueryPattern &&
          (previewResultState.isError() ? (
            <div>{previewResultState.getErrorMessage()}</div>
          ) : (
            previewResultSomeData && (
              <ColumnList
                queryResult={previewResultSomeData}
                columnFilter={columnFilter}
                onEditColumn={setColumnInEditor}
                onQueryAction={handleQueryAction}
              />
            )
          ))}
        {effectiveMode === 'sql' && (
          <>
            <TalariaQueryInput
              autoHeight={false}
              queryString={queryString}
              onQueryStringChange={onQueryStringChange}
              runeMode={false}
              columnMetadata={undefined}
            />
            {ingestPatternError && (
              <Callout className="pattern-error" intent={Intent.DANGER}>
                {ingestPatternError}
              </Callout>
            )}
          </>
        )}
      </div>
      {showRollupAnalysisPane && ingestQueryPattern && (
        <RollupAnalysisPane
          dimensions={ingestQueryPattern.dimensions}
          seedQuery={ingestQueryPatternToQuery(ingestQueryPattern, true)}
          queryResult={previewResultState.data}
          onEditColumn={setColumnInEditor}
          onQueryAction={handleQueryAction}
          onClose={() => setShowRollupAnalysisPane(false)}
        />
      )}
      {showAddExternal && (
        <ExternalConfigDialog
          onSetExternalConfig={() => {}}
          onClose={() => setShowAddExternal(false)}
        />
      )}
      {externalInEditor && (
        <ExternalConfigDialog
          initExternalConfig={externalInEditor}
          onSetExternalConfig={() => {}}
          onClose={() => setExternalInEditor(undefined)}
        />
      )}
      {addColumnType && ingestQueryPattern && (
        <ExpressionEditorDialog
          title="Add column"
          includeOutputName
          onSave={newExpression => {
            if (addColumnType === 'metric' && ingestQueryPattern.metrics) {
              updatePattern({
                ...ingestQueryPattern,
                metrics: ingestQueryPattern.metrics.concat(newExpression),
              });
            } else {
              updatePattern({
                ...ingestQueryPattern,
                dimensions: ingestQueryPattern.dimensions.concat(newExpression),
              });
            }
          }}
          onClose={() => setAddColumnType(undefined)}
        />
      )}
      {columnInEditor && ingestQueryPattern && (
        <ExpressionEditorDialog
          title="Edit column"
          includeOutputName
          expression={columnInEditor}
          onSave={newColumn =>
            updatePattern({
              ...ingestQueryPattern,
              dimensions: changeByString(ingestQueryPattern.dimensions, columnInEditor, newColumn),
              metrics: ingestQueryPattern.metrics
                ? changeByString(ingestQueryPattern.metrics, columnInEditor, newColumn)
                : undefined,
            })
          }
          onClose={() => setColumnInEditor(undefined)}
        />
      )}
      {showAddFilterEditor && ingestQueryPattern && (
        <ExpressionEditorDialog
          title="Add filter"
          onSave={newExpression =>
            updatePattern({
              ...ingestQueryPattern,
              filters: ingestQueryPattern.filters.concat(newExpression),
            })
          }
          onClose={() => setShowAddFilterEditor(false)}
        />
      )}
      {filterInEditor && ingestQueryPattern && (
        <ExpressionEditorDialog
          title="Edit filter"
          expression={filterInEditor}
          onSave={newFilter =>
            updatePattern({
              ...ingestQueryPattern,
              filters: change(ingestQueryPattern.filters, filterInEditor, newFilter),
            })
          }
          onDelete={() =>
            updatePattern({
              ...ingestQueryPattern,
              filters: without(ingestQueryPattern.filters, filterInEditor),
            })
          }
          onClose={() => setFilterInEditor(undefined)}
        />
      )}
      {showRollupConfirm && ingestQueryPattern && (
        <AsyncActionDialog
          action={async () => {
            await wait(100); // A hack to make it async. Revisit
            toggleRollup();
          }}
          confirmButtonText={`Yes - ${ingestQueryPattern.metrics ? 'disable' : 'enable'} rollup`}
          successText={`Rollup was ${
            ingestQueryPattern.metrics ? 'disabled' : 'enabled'
          }. Schema has been updated.`}
          failText="Could change rollup"
          intent={Intent.WARNING}
          onClose={() => setShowRollupConfirm(false)}
        >
          <p>{`Are you sure you want to ${
            ingestQueryPattern.metrics ? 'disable' : 'enable'
          } rollup?`}</p>
          <p>Making this change will reset any work you have done in this section.</p>
        </AsyncActionDialog>
      )}
    </div>
  );
};
