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

import { Icon, Menu, MenuItem } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { QueryResult, SqlAlias, SqlExpression, SqlLiteral, SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import { BracedText, TableCell } from '../../../../components';
import { Deferred } from '../../../../components/deferred/deferred';
import { ShowValueDialog } from '../../../../dialogs/show-value-dialog/show-value-dialog';
import {
  filterMap,
  getNumericColumnBraces,
  prettyPrintSql,
  QueryAction,
  stringifyValue,
} from '../../../../utils';
import { dataTypeToIcon } from '../../../query-view/query-utils';
import { ColumnActionMenu } from '../../column-action-menu/column-action-menu';

import './preview-table.scss';

function isDate(v: any): v is Date {
  return Boolean(v && typeof v.toISOString === 'function');
}

function isComparable(x: unknown): boolean {
  return x !== null && x !== '' && !isNaN(Number(x));
}

function getExpressionIfAlias(query: SqlQuery, selectIndex: number): string {
  const ex = query.getSelectExpressionForIndex(selectIndex);

  if (query.isRealOutputColumnAtSelectIndex(selectIndex)) {
    if (ex instanceof SqlAlias) {
      return String(ex.expression.prettify());
    } else {
      return '';
    }
  } else {
    return ex ? String(ex.prettify()) : '';
  }
}

export interface PreviewTableProps {
  queryResult: QueryResult;
  onQueryAction(action: QueryAction): void;
  columnFilter?(columnName: string): boolean;
  onEditColumn(expression: SqlExpression): void;
}

export const PreviewTable = React.memo(function PreviewTable(props: PreviewTableProps) {
  const { queryResult, onQueryAction, columnFilter, onEditColumn } = props;
  const [showValue, setShowValue] = useState<string>();

  const parsedQuery: SqlQuery = queryResult.sqlQuery!;
  if (!parsedQuery) return null;

  function hasFilterOnHeader(header: string, headerIndex: number): boolean {
    if (!parsedQuery || !parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) return false;

    return (
      parsedQuery.getEffectiveWhereExpression().containsColumn(header) ||
      parsedQuery.getEffectiveHavingExpression().containsColumn(header)
    );
  }

  function filterOnMenuItem(icon: IconName, clause: SqlExpression) {
    return (
      <MenuItem
        icon={icon}
        text={`Filter on: ${prettyPrintSql(clause)}`}
        onClick={() => {
          onQueryAction(q => q.addWhere(clause));
        }}
      />
    );
  }

  function getCellMenu(headerIndex: number, value: unknown) {
    const val = SqlLiteral.maybe(value);
    const showFullValueMenuItem = (
      <MenuItem
        icon={IconNames.EYE_OPEN}
        text="Show full value"
        onClick={() => {
          setShowValue(stringifyValue(value));
        }}
      />
    );

    let ex: SqlExpression | undefined;
    if (!parsedQuery.isAggregateSelectIndex(headerIndex)) {
      const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
      if (selectValue) {
        ex = selectValue.getUnderlyingExpression();
      }
    }

    return (
      <Menu>
        {ex && val && (
          <>
            {isComparable(value) && (
              <>
                {filterOnMenuItem(IconNames.FILTER_KEEP, ex.greaterThanOrEqual(val))}
                {filterOnMenuItem(IconNames.FILTER_KEEP, ex.lessThanOrEqual(val))}
              </>
            )}
            {filterOnMenuItem(IconNames.FILTER_KEEP, ex.equal(val))}
            {filterOnMenuItem(IconNames.FILTER_REMOVE, ex.unequal(val))}
          </>
        )}
        {showFullValueMenuItem}
      </Menu>
    );
  }

  const numericColumnBraces = getNumericColumnBraces(queryResult);
  return (
    <div className="preview-table">
      <ReactTable
        data={queryResult.rows as any[][]}
        noDataText={queryResult.rows.length ? '' : 'Preview returned no data'}
        defaultPageSize={50}
        showPagination={false}
        sortable={false}
        columns={filterMap(queryResult.header, (column, i) => {
          const h = column.name;
          if (columnFilter && !columnFilter(h)) return;

          const effectiveType = column.isTimeColumn() ? column.sqlType : column.nativeType;
          const icon = effectiveType ? dataTypeToIcon(effectiveType) : IconNames.BLANK;

          const columnClassName = parsedQuery.isAggregateSelectIndex(i)
            ? classNames('metric', {
                'first-metric': i > 0 && !parsedQuery.isAggregateSelectIndex(i - 1),
              })
            : classNames(
                column.isTimeColumn() ? 'timestamp' : 'dimension',
                column.sqlType?.toLowerCase(),
              );

          return {
            Header() {
              return (
                <Popover2
                  className="clickable-cell"
                  content={
                    <Deferred
                      content={() => {
                        const expression = parsedQuery.getSelectExpressionForIndex(i);
                        if (!expression) return null;

                        return (
                          <ColumnActionMenu
                            column={column}
                            headerIndex={i}
                            queryResult={queryResult}
                            filtered={parsedQuery.getWhereExpression()?.containsColumn(column.name)}
                            grouped={
                              parsedQuery.hasGroupBy()
                                ? parsedQuery.isGroupedSelectIndex(i)
                                : undefined
                            }
                            onEditColumn={onEditColumn}
                            onQueryAction={onQueryAction}
                          />
                        );
                      }}
                    />
                  }
                >
                  <div>
                    <div className="output-name">
                      <Icon className="type-icon" icon={icon} iconSize={12} />
                      {h}
                      {hasFilterOnHeader(h, i) && (
                        <Icon className="filter-icon" icon={IconNames.FILTER} iconSize={14} />
                      )}
                    </div>
                    <div className="formula">{getExpressionIfAlias(parsedQuery, i)}</div>
                  </div>
                </Popover2>
              );
            },
            headerClassName: columnClassName,
            className: columnClassName,
            accessor: String(i),
            Cell(row) {
              const value = row.value;

              return (
                <div>
                  <Popover2 content={<Deferred content={() => getCellMenu(i, value)} />}>
                    {numericColumnBraces[i] ? (
                      <BracedText
                        text={isDate(value) ? value.toISOString() : String(value)}
                        braces={numericColumnBraces[i]}
                        padFractionalPart
                      />
                    ) : (
                      <TableCell value={value} unlimited />
                    )}
                  </Popover2>
                </div>
              );
            },
            width: column.isTimeColumn() ? 180 : 120,
          };
        })}
      />
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
    </div>
  );
});
