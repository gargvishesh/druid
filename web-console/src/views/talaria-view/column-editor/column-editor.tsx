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

import { Button, FormGroup, InputGroup, Intent, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, SqlExpression, SqlFunction } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AppToaster } from '../../../singletons';
import { filterMap, QueryAction } from '../../../utils';
import { TalariaQueryInput } from '../talaria-query-input/talaria-query-input';

import './column-editor.scss';

function cast(ex: SqlExpression, as: string): SqlExpression {
  return SqlExpression.parse(`CAST(${ex} AS ${as})`);
}

const CAST_TARGETS: string[] = ['VARCHAR', 'BIGINT', 'DOUBLE'];

interface ColumnEditorProps {
  expression?: SqlExpression;
  onApply(expression: SqlExpression | undefined): void;
  onCancel(): void;
  dirty(): void;

  queryResult: QueryResult | undefined;
  headerIndex: number;
  onQueryAction(action: QueryAction): void;
}

export const ColumnEditor = React.memo(function ExpressionEditor(props: ColumnEditorProps) {
  const { expression, onApply, onCancel, dirty, queryResult, headerIndex, onQueryAction } = props;

  const [outputName, setOutputName] = useState<string | undefined>();
  const [expressionString, setExpressionString] = useState<string | undefined>();

  const effectiveOutputName = outputName ?? (expression?.getOutputName() || '');
  const effectiveExpressionString =
    expressionString ?? (expression?.getUnderlyingExpression()?.toString() || '');

  let castButton: JSX.Element | undefined;

  const sqlQuery = queryResult?.sqlQuery;
  if (queryResult && sqlQuery && headerIndex !== -1) {
    const column = queryResult.header[headerIndex];

    const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);

    if (expression && column.sqlType !== 'TIMESTAMP') {
      // Casts
      const selectExpression = sqlQuery.getSelectExpressionForIndex(headerIndex);
      if (selectExpression) {
        const castMenuItems: JSX.Element[] = [];

        const underlyingExpression = selectExpression.getUnderlyingExpression();
        if (
          underlyingExpression instanceof SqlFunction &&
          underlyingExpression.getEffectiveFunctionName() === 'CAST'
        ) {
          castMenuItems.push(
            <MenuItem
              key="remove-cast"
              icon={IconNames.CROSS}
              text="Remove cast"
              onClick={() => {
                if (!selectExpression || !underlyingExpression) return;
                onQueryAction(q =>
                  q.changeSelect(
                    headerIndex,
                    underlyingExpression.getArg(0)!.as(selectExpression.getOutputName()),
                  ),
                );
              }}
            />,
          );
        }

        castMenuItems.push(
          ...filterMap(CAST_TARGETS, as => {
            if (as === column.sqlType) return;
            return (
              <MenuItem
                key={as}
                text={as}
                onClick={() => {
                  if (!selectExpression) return;
                  onQueryAction(q =>
                    q.changeSelect(
                      headerIndex,
                      cast(selectExpression.getUnderlyingExpression(), as).as(
                        selectExpression.getOutputName(),
                      ),
                    ),
                  );
                }}
              />
            );
          }),
        );

        castButton = (
          <Popover2 content={<Menu>{castMenuItems}</Menu>}>
            <Button icon={IconNames.EXCHANGE} text="Cast to..." />
          </Popover2>
        );
      }
    }
  }

  return (
    <div className="column-editor">
      <div className="title">{expression ? 'Edit column' : 'Add column'}</div>
      <FormGroup label="Name">
        <InputGroup
          value={effectiveOutputName}
          onChange={e => {
            if (!outputName) dirty();
            setOutputName(e.target.value);
          }}
        />
      </FormGroup>
      <FormGroup label="SQL expression">
        <TalariaQueryInput
          autoHeight={false}
          showGutter={false}
          placeholder="expression"
          queryString={effectiveExpressionString}
          onQueryStringChange={f => {
            if (!expressionString) dirty();
            setExpressionString(f);
          }}
          runeMode={false}
          columnMetadata={undefined}
        />
      </FormGroup>
      {castButton && <FormGroup>{castButton}</FormGroup>}
      <div className="apply-cancel-buttons">
        {expression && (
          <Button
            className="delete"
            icon={IconNames.TRASH}
            intent={Intent.DANGER}
            onClick={() => {
              onApply(undefined);
              onCancel();
            }}
          />
        )}
        <Button text="Cancel" onClick={onCancel} />
        <Button
          text="Apply"
          intent={Intent.PRIMARY}
          disabled={!outputName && !expressionString}
          onClick={() => {
            let newExpression: SqlExpression;
            try {
              newExpression = SqlExpression.parse(effectiveExpressionString);
            } catch (e) {
              AppToaster.show({
                message: `Could not parse SQL expression: ${e.message}`,
                intent: Intent.DANGER,
              });
              return;
            }

            if (newExpression.getOutputName() !== effectiveOutputName) {
              newExpression = newExpression.as(effectiveOutputName);
            }

            onApply(newExpression);
            onCancel();
          }}
        />
      </div>
    </div>
  );
});
