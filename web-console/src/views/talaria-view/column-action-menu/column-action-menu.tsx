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

import { Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Column, QueryResult, SqlExpression, SqlFunction } from 'druid-query-toolkit';
import React from 'react';

import { possibleDruidFormatForValues, TIME_COLUMN } from '../../../druid-models';
import { oneOf, QueryAction } from '../../../utils';
import { convertToGroupByExpression, timeFormatToSql } from '../sql-utils';
import { TimeFloorMenuItem } from '../time-floor-menu-item/time-floor-menu-item';

export interface ColumnActionMenuProps {
  column: Column;
  headerIndex: number;
  queryResult: QueryResult;
  filtered?: boolean;
  grouped?: boolean;
  onEditColumn?(expression?: SqlExpression): void;
  onQueryAction(action: QueryAction): void;
}

export function ColumnActionMenu(props: ColumnActionMenuProps) {
  const { column, headerIndex, queryResult, filtered, grouped, onEditColumn, onQueryAction } =
    props;

  const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);
  if (!expression) return null;

  const header = column.name;
  const type = column.sqlType || column.nativeType;

  const menuItems: JSX.Element[] = [<MenuDivider key="title" title={header} />];

  if (filtered) {
    menuItems.push(
      <MenuItem
        key="remove_where"
        icon={IconNames.FILTER_REMOVE}
        text="Remove from WHERE clause"
        onClick={() => {
          onQueryAction(q =>
            q.changeWhereExpression(q.getWhereExpression()?.removeColumnFromAnd(header)),
          );
        }}
      />,
    );
  }

  if (onEditColumn) {
    menuItems.push(
      <MenuItem
        key="edit_column"
        icon={IconNames.EDIT}
        text="Edit column"
        onClick={() => onEditColumn(expression)}
      />,
    );
  }

  if (column.sqlType === 'TIMESTAMP') {
    menuItems.push(
      <TimeFloorMenuItem
        key="time_floor"
        expression={expression}
        onChange={expression => {
          onQueryAction(q => q.changeSelect(headerIndex, expression));
        }}
      />,
    );

    if (!column.isTimeColumn()) {
      menuItems.push(
        <MenuItem
          key="declare_time"
          icon={IconNames.TIME}
          text="Use as the primary time column"
          onClick={() => {
            onQueryAction(q =>
              q.removeSelectIndex(headerIndex).addSelect(expression.as(TIME_COLUMN), {
                insertIndex: 0,
                addToGroupBy: q.hasGroupBy() ? 'start' : undefined,
              }),
            );
          }}
        />,
      );
    }
  } else {
    // Not a time column -------------------------------------------
    const values = queryResult.rows.map(row => row[headerIndex]);
    const possibleDruidFormat = possibleDruidFormatForValues(values);
    const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;

    if (formatSql) {
      const newSelectExpression = formatSql.fillPlaceholders([
        expression.getUnderlyingExpression(),
      ]);

      menuItems.push(
        <MenuItem
          key="parse_time"
          icon={IconNames.TIME}
          text={`Parse as '${possibleDruidFormat}'`}
          onClick={() => {
            const outputName = expression?.getOutputName();
            if (!outputName) return;
            onQueryAction(q => q.changeSelect(headerIndex, newSelectExpression.as(outputName)));
          }}
        />,
        <MenuItem
          key="parse_time_and_make_primary"
          icon={IconNames.TIME}
          text={`Parse as '${possibleDruidFormat}' and use as the primary time column`}
          onClick={() => {
            onQueryAction(q =>
              q.removeSelectIndex(headerIndex).addSelect(newSelectExpression.as(TIME_COLUMN), {
                insertIndex: 0,
                addToGroupBy: q.hasGroupBy() ? 'start' : undefined,
              }),
            );
          }}
        />,
      );
    }

    if (typeof grouped === 'boolean') {
      if (grouped) {
        const convertToAggregate = (aggregates: SqlExpression[]) => {
          onQueryAction(q =>
            q.removeOutputColumn(header).applyForEach(aggregates, (q, aggregate) =>
              q.addSelect(aggregate, {
                insertIndex: 'last',
              }),
            ),
          );
        };

        const underlyingSelectExpression = expression.getUnderlyingExpression();

        menuItems.push(
          <MenuItem key="convert_to_metric" icon={IconNames.EXCHANGE} text="Convert to metric">
            {oneOf(type, 'LONG', 'FLOAT', 'DOUBLE', 'BIGINT') && (
              <>
                <MenuItem
                  text="Convert to SUM(...)"
                  onClick={() => {
                    convertToAggregate([
                      SqlFunction.simple('SUM', [underlyingSelectExpression]).as(`sum_${header}`),
                    ]);
                  }}
                />
                <MenuItem
                  text="Convert to MIN(...)"
                  onClick={() => {
                    convertToAggregate([
                      SqlFunction.simple('MIN', [underlyingSelectExpression]).as(`min_${header}`),
                    ]);
                  }}
                />
                <MenuItem
                  text="Convert to MAX(...)"
                  onClick={() => {
                    convertToAggregate([
                      SqlFunction.simple('MAX', [underlyingSelectExpression]).as(`max_${header}`),
                    ]);
                  }}
                />
                <MenuItem
                  text="Convert to SUM(...), MIN(...), and MAX(...)"
                  onClick={() => {
                    convertToAggregate([
                      SqlFunction.simple('SUM', [underlyingSelectExpression]).as(`sum_${header}`),
                      SqlFunction.simple('MIN', [underlyingSelectExpression]).as(`min_${header}`),
                      SqlFunction.simple('MAX', [underlyingSelectExpression]).as(`max_${header}`),
                    ]);
                  }}
                />
              </>
            )}
            <MenuItem
              text="Convert to APPROX_COUNT_DISTINCT_DS_HLL(...)"
              onClick={() => {
                convertToAggregate([
                  SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_HLL', [
                    underlyingSelectExpression,
                  ]).as(`unique_${header}`),
                ]);
              }}
            />
            <MenuItem
              text="Convert to APPROX_COUNT_DISTINCT_DS_THETA(...)"
              onClick={() => {
                convertToAggregate([
                  SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_THETA', [
                    underlyingSelectExpression,
                  ]).as(`unique_${header}`),
                ]);
              }}
            />
            <MenuItem
              text="Convert to APPROX_COUNT_DISTINCT_BUILTIN(...)"
              onClick={() => {
                convertToAggregate([
                  SqlFunction.simple('APPROX_COUNT_DISTINCT_BUILTIN', [
                    underlyingSelectExpression,
                  ]).as(`unique_${header}`),
                ]);
              }}
            />
          </MenuItem>,
        );
      } else {
        const groupByExpression = convertToGroupByExpression(expression);
        if (groupByExpression) {
          menuItems.push(
            <MenuItem
              key="convert_to_dimension"
              icon={IconNames.EXCHANGE}
              text="Convert to dimension"
              onClick={() => {
                onQueryAction(q =>
                  q.removeOutputColumn(header).addSelect(groupByExpression, {
                    insertIndex: 'last-grouping',
                    addToGroupBy: 'end',
                  }),
                );
              }}
            />,
          );
        }
      }
    }
  }

  menuItems.push(
    <MenuItem
      key="remove_column"
      icon={IconNames.CROSS}
      text="Remove column"
      onClick={() => {
        onQueryAction(q => q.removeOutputColumn(header));
      }}
    />,
  );

  return <Menu>{menuItems}</Menu>;
}
