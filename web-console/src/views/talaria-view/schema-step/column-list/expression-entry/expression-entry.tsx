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

import { Icon } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { Column, QueryResult, SqlExpression, SqlRef } from 'druid-query-toolkit';
import React from 'react';

import { Deferred } from '../../../../../components/deferred/deferred';
import { QueryAction } from '../../../../../utils';
import { dataTypeToIcon } from '../../../../query-view/query-utils';
import { ColumnActionMenu } from '../../../column-action-menu/column-action-menu';

import './expression-entry.scss';

interface ExpressionEntryProps {
  column: Column;
  headerIndex: number;
  queryResult: QueryResult;
  filtered?: boolean;
  grouped?: boolean;
  onEditColumn(expression: SqlExpression): void;
  onQueryAction(action: QueryAction): void;
}

export const ExpressionEntry = function ExpressionEntry(props: ExpressionEntryProps) {
  const {
    column,
    headerIndex,
    queryResult,
    filtered,
    grouped,
    onEditColumn,
    onQueryAction,
  } = props;

  const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);
  if (!expression) return null;

  const effectiveType = column.isTimeColumn() ? column.sqlType : column.nativeType;
  const icon = effectiveType ? dataTypeToIcon(effectiveType) : IconNames.BLANK;

  return (
    <Popover2
      className={classNames('expression-entry')}
      position={grouped ? 'left' : 'right'}
      content={
        <Deferred
          content={() => (
            <ColumnActionMenu
              column={column}
              headerIndex={headerIndex}
              queryResult={queryResult}
              filtered={filtered}
              grouped={grouped}
              onEditColumn={onEditColumn}
              onQueryAction={onQueryAction}
            />
          )}
        />
      }
    >
      <div
        className={
          grouped === false
            ? classNames('expression-entry-inner', 'metric')
            : classNames(
                'expression-entry-inner',
                column.isTimeColumn() ? 'timestamp' : 'dimension',
                column.sqlType?.toLowerCase(),
              )
        }
      >
        {icon && <Icon className="type-icon" icon={icon} iconSize={14} />}
        <div className="output-name">
          {expression.getOutputName() || 'EXPR?'}
          <span className="type-name">{` :: ${effectiveType}`}</span>
        </div>
        {!(expression instanceof SqlRef) && (
          <div className="expression">
            {expression
              .getUnderlyingExpression()
              .prettify({ keywordCasing: 'preserve' })
              .toString()}
          </div>
        )}
      </div>
    </Popover2>
  );
};
