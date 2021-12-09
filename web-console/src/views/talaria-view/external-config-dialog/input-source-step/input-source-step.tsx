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

import { Button, Icon, Intent, TextArea } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { QueryResult, QueryRunner } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AutoForm, CenterMessage, Loader } from '../../../../components';
import {
  guessInputFormat,
  InputFormat,
  InputSource,
  MAX_INLINE_DATA_LENGTH,
} from '../../../../druid-models';
import { useQueryManager } from '../../../../hooks';
import {
  externalConfigToTableExpression,
  INPUT_SOURCE_FIELDS,
  TalariaSummary,
} from '../../../../talaria-models';
import { deepSet, DruidError, IntermediateQueryState } from '../../../../utils';
import {
  getTaskIdFromQueryResults,
  killTaskOnCancel,
  talariaBackgroundResultStatusCheck,
} from '../../talaria-utils';

import './input-source-step.scss';

const queryRunner = new QueryRunner();

export interface InputSourceStepProps {
  initInputSource: Partial<InputSource>;
  onSet(inputSource: InputSource, inputFormat: InputFormat): void;
  onBack(): void;
}

export const InputSourceStep = React.memo(function InputSourceStep(props: InputSourceStepProps) {
  const { initInputSource, onSet, onBack } = props;

  const [inputSource, setInputSource] = useState<Partial<InputSource>>(initInputSource);
  const [inputSourceToSample, setInputSourceToSample] = useState<InputSource | undefined>(
    AutoForm.isValidModel(initInputSource, INPUT_SOURCE_FIELDS) ? initInputSource : undefined,
  );
  const inlineMode = inputSource?.type === 'inline';

  const [connectResultState] = useQueryManager<InputSource, QueryResult, TalariaSummary>({
    query: inputSourceToSample,
    processQuery: async (inputSource: InputSource, cancelToken) => {
      const query = `SELECT raw FROM ${externalConfigToTableExpression({
        inputSource,
        inputFormat: { type: 'regex', pattern: '(.*)', columns: ['raw'] },
        columns: [{ name: 'raw', type: 'string' }],
      })} LIMIT 100`;

      let result: QueryResult;
      try {
        result = await queryRunner.runQuery({
          query,
          extraQueryContext: {
            talaria: true,
          },
          cancelToken,
        });
      } catch (e) {
        throw new DruidError(e);
      }

      const taskId = getTaskIdFromQueryResults(result);
      if (!taskId) {
        throw new Error('Unexpected result, could not determine taskId');
      }

      killTaskOnCancel(taskId, cancelToken);

      return new IntermediateQueryState(TalariaSummary.init(taskId));
    },
    backgroundStatusCheck: talariaBackgroundResultStatusCheck,
  });

  return (
    <div className="input-source-step">
      <div className="preview">
        {inlineMode ? (
          <TextArea
            className="inline-data"
            placeholder="Paste your data here"
            value={inputSource?.data || ''}
            onChange={(e: any) => {
              const stringValue = e.target.value.substr(0, MAX_INLINE_DATA_LENGTH);
              setInputSource(deepSet(inputSource, 'data', stringValue));
            }}
          />
        ) : (
          <>
            {connectResultState.isInit() && (
              <CenterMessage>
                Please fill out the fields on the right sidebar to get started{' '}
                <Icon icon={IconNames.ARROW_RIGHT} />
              </CenterMessage>
            )}
            {connectResultState.data && (
              <TextArea
                className="raw-lines"
                readOnly
                value={connectResultState.data.rows.map((r: any) => r[0]).join('\n')}
              />
            )}
            {connectResultState.isLoading() && <Loader />}
            {connectResultState.error && (
              <CenterMessage>{`Error: ${connectResultState.getErrorMessage()}`}</CenterMessage>
            )}
          </>
        )}
      </div>
      <div className="config">
        <AutoForm fields={INPUT_SOURCE_FIELDS} model={inputSource} onChange={setInputSource} />
        <Button
          text="Apply"
          intent={Intent.PRIMARY}
          onClick={() => {
            setInputSourceToSample(inputSource as any);
          }}
        />
        <Button className="back" text="Back" icon={IconNames.ARROW_LEFT} onClick={onBack} />
        <Button
          className="next"
          text="Next"
          rightIcon={IconNames.ARROW_RIGHT}
          intent={Intent.PRIMARY}
          disabled={!connectResultState.data}
          onClick={() => {
            const sampleData = connectResultState.data;
            if (!sampleData) return;
            onSet(inputSource as any, guessInputFormat(sampleData.rows.map((r: any) => r[0])));
          }}
        />
      </div>
    </div>
  );
});
