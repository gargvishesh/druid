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
  Callout,
  Card,
  FormGroup,
  HTMLSelect,
  Intent,
  ProgressBar,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import { QueryResult } from 'druid-query-toolkit';
import React, { useEffect, useState } from 'react';

import { AutoForm } from '../../../../components';
import { ShowValueDialog } from '../../../../dialogs/show-value-dialog/show-value-dialog';
import {
  getIngestionImage,
  getIngestionTitle,
  guessInputFormat,
  InputFormat,
  InputSource,
} from '../../../../druid-models';
import { useQueryManager } from '../../../../hooks';
import { UrlBaser } from '../../../../singletons';
import {
  Execution,
  ExecutionError,
  externalConfigToTableExpression,
  INPUT_SOURCE_FIELDS,
} from '../../../../workbench-models';
import {
  executionBackgroundResultStatusCheck,
  extractQueryResults,
  submitTaskQuery,
} from '../../execution-utils';

import { InputSourceInfo } from './input-source-info';

import './input-source-step.scss';

interface ExampleInputSource {
  name: string;
  description: string;
  inputSource: InputSource;
}

const EXAMPLE_INPUT_SOURCES: ExampleInputSource[] = [
  {
    name: 'Wikipedia',
    description: 'JSON data representing one day of wikipedia edits',
    inputSource: {
      type: 'http',
      uris: ['https://druid.apache.org/data/wikipedia.json.gz'],
    },
  },
  {
    name: 'Koalas one day',
    description: 'JSON data representing one day of events from KoalasToTheMax.com',
    inputSource: {
      type: 'http',
      uris: ['https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz'],
    },
  },
];

export interface InputSourceStepProps {
  initInputSource: Partial<InputSource> | undefined;
  onSet(inputSource: InputSource, inputFormat: InputFormat): void;
}

export const InputSourceStep = React.memo(function InputSourceStep(props: InputSourceStepProps) {
  const { initInputSource, onSet } = props;

  const [stackToShow, setStackToShow] = useState<string | undefined>();
  const [inputSource, setInputSource] = useState<Partial<InputSource> | string | undefined>(
    initInputSource,
  );
  const exampleInputSource = EXAMPLE_INPUT_SOURCES.find(({ name }) => name === inputSource);

  const [connectResultState, connectQueryManager] = useQueryManager<
    InputSource,
    QueryResult,
    Execution
  >({
    processQuery: async (inputSource: InputSource, cancelToken) => {
      const externExpression = externalConfigToTableExpression({
        inputSource,
        inputFormat: {
          type: 'regex',
          pattern: '([\\s\\S]*)',
          listDelimiter: '56616469-6de2-9da4-efb8-8f416e6e6965', // Just a UUID to disable the list delimiter, let's hope we do not see this UUID in the data
          columns: ['raw'],
        },
        signature: [{ name: 'raw', type: 'string' }],
      });

      return extractQueryResults(
        await submitTaskQuery({
          query: `SELECT REPLACE(raw, U&'\\0000', '') AS "raw" FROM ${externExpression}`, // Make sure to remove possible \u0000 chars as they are not allowed and will produce an InvalidNullByte error message
          context: {
            sqlOuterLimit: 100,
          },
          cancelToken,
        }),
      );
    },
    backgroundStatusCheck: executionBackgroundResultStatusCheck,
  });

  useEffect(() => {
    const sampleData = connectResultState.data;
    if (!sampleData) return;
    onSet(
      exampleInputSource?.inputSource || (inputSource as any),
      guessInputFormat(sampleData.rows.map((r: any) => r[0])),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectResultState]);

  const effectiveType = typeof inputSource === 'string' ? 'example' : inputSource?.type;
  function renderIngestionCard(type: string): JSX.Element | undefined {
    const selected = type === effectiveType;
    return (
      <Card
        className={classNames({ selected, disabled: false })}
        interactive
        elevation={1}
        onClick={() => {
          if (selected) {
            setInputSource(undefined);
          } else {
            setInputSource(type === 'example' ? EXAMPLE_INPUT_SOURCES[0].name : { type });
          }
        }}
      >
        <img
          src={UrlBaser.base(`/assets/${getIngestionImage(type as any)}.png`)}
          alt={`Ingestion tile for ${type}`}
        />
        <p>
          {getIngestionTitle(type === 'example' ? 'example' : (`index_parallel:${type}` as any))}
        </p>
      </Card>
    );
  }

  const connectResultError = connectResultState.error;
  return (
    <div className="input-source-step">
      <div className="main">
        <div className="ingestion-cards">
          {renderIngestionCard('s3')}
          {renderIngestionCard('azure')}
          {renderIngestionCard('google')}
          {renderIngestionCard('hdfs')}
          {renderIngestionCard('http')}
          {renderIngestionCard('local')}
          {renderIngestionCard('inline')}
          {renderIngestionCard('example')}
        </div>
      </div>
      <div className="config">
        {typeof inputSource === 'string' ? (
          <>
            <FormGroup label="Select example dataset">
              <HTMLSelect fill value={inputSource} onChange={e => setInputSource(e.target.value)}>
                {EXAMPLE_INPUT_SOURCES.map((e, i) => (
                  <option key={i} value={e.name}>
                    {e.name}
                  </option>
                ))}
              </HTMLSelect>
            </FormGroup>
            {exampleInputSource && (
              <>
                <FormGroup>
                  <Callout>{exampleInputSource.description}</Callout>
                </FormGroup>
                <Button
                  className="next"
                  text={connectResultState.isLoading() ? 'Loading...' : 'Use example'}
                  rightIcon={IconNames.ARROW_RIGHT}
                  intent={Intent.PRIMARY}
                  disabled={connectResultState.isLoading()}
                  onClick={() => {
                    connectQueryManager.runQuery(exampleInputSource.inputSource);
                  }}
                />
              </>
            )}
          </>
        ) : inputSource ? (
          <>
            <FormGroup>
              <Callout>
                <InputSourceInfo inputSource={inputSource} />
              </Callout>
            </FormGroup>
            <AutoForm fields={INPUT_SOURCE_FIELDS} model={inputSource} onChange={setInputSource} />
            <Button
              className="next"
              text={connectResultState.isLoading() ? 'Loading...' : 'Connect data'}
              rightIcon={IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              disabled={
                !AutoForm.isValidModel(inputSource, INPUT_SOURCE_FIELDS) ||
                connectResultState.isLoading()
              }
              onClick={() => {
                if (!AutoForm.isValidModel(inputSource, INPUT_SOURCE_FIELDS)) return;
                connectQueryManager.runQuery(inputSource);
              }}
            />
          </>
        ) : (
          <FormGroup>
            <Callout>
              <p>Please specify where your raw data is located</p>
            </Callout>
          </FormGroup>
        )}
        {connectResultState.isLoading() && (
          <FormGroup>
            <ProgressBar intent={Intent.PRIMARY} />
          </FormGroup>
        )}
        {connectResultError && (
          <FormGroup>
            <Callout intent={Intent.DANGER}>
              <p>{connectResultState.getErrorMessage()}</p>
              {(connectResultError as any).executionError && (
                <p>
                  <a
                    onClick={() => {
                      setStackToShow(
                        ((connectResultError as any).executionError as ExecutionError)
                          .exceptionStackTrace,
                      );
                    }}
                  >
                    Stack trace
                  </a>
                </p>
              )}
            </Callout>
          </FormGroup>
        )}
      </div>
      {stackToShow && (
        <ShowValueDialog
          size="large"
          title="Full stack trace"
          onClose={() => setStackToShow(undefined)}
          str={stackToShow}
        />
      )}
    </div>
  );
});
