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

import { Button, Icon, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { AutoForm, CenterMessage, Loader } from '../../../../components';
import {
  guessColumnTypeFromHeaderAndRows,
  INPUT_FORMAT_FIELDS,
  InputFormat,
  InputSource,
  PLACEHOLDER_TIMESTAMP_SPEC,
} from '../../../../druid-models';
import { useQueryManager } from '../../../../hooks';
import { ExternalConfigColumn } from '../../../../talaria-models';
import { deepSet, EMPTY_ARRAY } from '../../../../utils';
import {
  headerAndRowsFromSampleResponse,
  postToSampler,
  SampleHeaderAndRows,
  SampleSpec,
} from '../../../../utils/sampler';
import { ParseDataTable } from '../../../load-data-view/parse-data-table/parse-data-table';

import './input-format-step.scss';

const noop = () => {};

export interface InputFormatStepProps {
  inputSource: InputSource;
  initInputFormat: Partial<InputFormat>;
  doneButton: boolean;
  onSet(inputFormat: InputFormat, columns: ExternalConfigColumn[]): void;
  onBack(): void;
}

export const InputFormatStep = React.memo(function InputFormatStep(props: InputFormatStepProps) {
  const { inputSource, initInputFormat, doneButton, onSet, onBack } = props;

  const [inputFormat, setInputFormat] = useState<Partial<InputFormat>>(initInputFormat);
  const [inputFormatToSample, setInputFormatToSample] = useState<InputFormat | undefined>(
    AutoForm.isValidModel(initInputFormat, INPUT_FORMAT_FIELDS) ? initInputFormat : undefined,
  );

  const [parseQueryState] = useQueryManager<InputFormat, SampleHeaderAndRows>({
    query: inputFormatToSample,
    processQuery: async (inputFormat: InputFormat) => {
      const sampleSpec: SampleSpec = {
        type: 'index_parallel',
        spec: {
          ioConfig: {
            type: 'index_parallel',
            inputSource,
            inputFormat: deepSet(inputFormat, 'keepNullColumns', true),
          },
          dataSchema: {
            dataSource: 'sample',
            timestampSpec: PLACEHOLDER_TIMESTAMP_SPEC,
            dimensionsSpec: {},
          },
        },
        samplerConfig: {
          numRows: 50,
          timeoutMs: 15000,
        },
      };

      const sampleResponse = await postToSampler(sampleSpec, 'should-be-talaria');

      return headerAndRowsFromSampleResponse({ sampleResponse, ignoreTimeColumn: true });
    },
  });

  return (
    <div className="input-format-step">
      <div className="preview">
        {parseQueryState.isInit() && (
          <CenterMessage>
            Please fill out the fields on the right sidebar to get started{' '}
            <Icon icon={IconNames.ARROW_RIGHT} />
          </CenterMessage>
        )}
        {parseQueryState.isLoading() && <Loader />}
        {parseQueryState.error && (
          <CenterMessage>{`Error: ${parseQueryState.getErrorMessage()}`}</CenterMessage>
        )}
        {parseQueryState.data && (
          <ParseDataTable
            sampleData={parseQueryState.data}
            columnFilter=""
            canFlatten={false}
            flattenedColumnsOnly={false}
            flattenFields={EMPTY_ARRAY}
            onFlattenFieldSelect={noop}
          />
        )}
      </div>
      <div className="config">
        <AutoForm fields={INPUT_FORMAT_FIELDS} model={inputFormat} onChange={setInputFormat} />
        <Button
          text="Apply"
          intent={Intent.PRIMARY}
          disabled={
            !AutoForm.isValidModel(inputFormat, INPUT_FORMAT_FIELDS) ||
            inputFormatToSample === inputFormat
          }
          onClick={() => {
            setInputFormatToSample(inputFormat as any);
          }}
        />
        <Button className="back" text="Back" icon={IconNames.ARROW_LEFT} onClick={onBack} />
        <Button
          className="next"
          text={doneButton ? 'Done' : 'Next'}
          rightIcon={doneButton ? IconNames.TICK : IconNames.ARROW_RIGHT}
          intent={Intent.PRIMARY}
          disabled={!parseQueryState.data}
          onClick={() => {
            const sampleData = parseQueryState.data;
            if (!sampleData) return;
            onSet(
              inputFormat as any,
              sampleData.header.map(name => ({
                name,
                type: guessColumnTypeFromHeaderAndRows(
                  sampleData,
                  name,
                  initInputFormat.type !== 'json',
                ),
              })),
            );
          }}
        />
      </div>
    </div>
  );
});
