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

import { Button, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { TalariaQueryInput } from '../talaria-query-input/talaria-query-input';

import './expression-editor.scss';

interface ExpressionEditorProps {
  expression?: SqlExpression;
  onApply(expression: SqlExpression | undefined): void;
  onCancel(): void;
}

export const ExpressionEditor = React.memo(function ExpressionEditor(props: ExpressionEditorProps) {
  const { expression, onApply, onCancel } = props;

  const [outputName, setOutputName] = useState<string>(() => expression?.getOutputName() || '');
  const [formula, setFormula] = useState<string>(
    () => expression?.getUnderlyingExpression()?.toString() || '',
  );

  const parsedExpression = formula ? SqlExpression.maybeParse(formula) : undefined;

  return (
    <div className="expression-editor">
      <div className="title">{expression ? 'Edit column' : 'Add column'}</div>
      <FormGroup label="Column name">
        <InputGroup value={outputName} onChange={e => setOutputName(e.target.value)} />
      </FormGroup>
      <FormGroup label="Formula">
        <TalariaQueryInput
          autoHeight={false}
          showGutter={false}
          placeholder="expression"
          queryString={formula}
          onQueryStringChange={setFormula}
          runeMode={false}
          columnMetadata={undefined}
        />
      </FormGroup>
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
          disabled={!parsedExpression}
          onClick={() => {
            if (!parsedExpression) return;
            let newExpression = parsedExpression;
            if (newExpression.getOutputName() !== outputName) {
              newExpression = newExpression.as(outputName);
            }
            onApply(newExpression);
            onCancel();
          }}
        />
      </div>
    </div>
  );
});
