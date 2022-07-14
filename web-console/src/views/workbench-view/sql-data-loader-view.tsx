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

import { Button, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { useLocalStorageState, useQueryManager } from '../../hooks';
import { getContextFromSqlQuery, LocalStorageKeys } from '../../utils';
import {
  Execution,
  ExternalConfig,
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
} from '../../workbench-models';

import { ExecutionProgressBarPane } from './execution-progress-bar-pane/execution-progress-bar-pane';
import { ExecutionStagesPane } from './execution-stages-pane/execution-stages-pane';
import { executionBackgroundStatusCheck, submitTaskQuery } from './execution-utils';
import { InputFormatStep } from './input-format-step/input-format-step';
import { InputSourceStep } from './input-source-step/input-source-step';
import { SchemaStep } from './schema-step/schema-step';
import { TitleFrame } from './title-frame/title-frame';

import './sql-data-loader-view.scss';

export interface SqlDataLoaderViewProps {
  goToQuery: (initSql: string) => void;
}

export const SqlDataLoaderView = React.memo(function SqlDataLoaderView(
  props: SqlDataLoaderViewProps,
) {
  const { goToQuery } = props;
  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>({});
  const [queryString, setQueryString] = useLocalStorageState<string>(
    LocalStorageKeys.SQL_DATA_LOADER_QUERY,
    '',
  );
  const [showLiveReports, setShowLiveReports] = useState(false);

  const { inputSource, inputFormat } = externalConfigStep;

  const [insertResultState, insertQueryManager] = useQueryManager<string, Execution, Execution>({
    processQuery: async (queryString: string, cancelToken) => {
      const insertDatasource = SqlQuery.parse(queryString).getInsertIntoTable()?.getTable();
      if (!insertDatasource) throw new Error(`Must have insert datasource`);

      return await submitTaskQuery({
        query: queryString,
        context: getContextFromSqlQuery(queryString),
        cancelToken,
      });
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
  });

  return (
    <div className="sql-data-loader-view">
      {insertResultState.isInit() && (
        <>
          {queryString ? (
            <SchemaStep
              queryString={queryString}
              onQueryStringChange={setQueryString}
              goToQuery={() => goToQuery(queryString)}
              onBack={() => setQueryString('')}
              onDone={() => {
                console.log('Ingesting:', queryString);
                insertQueryManager.runQuery(queryString);
              }}
            />
          ) : inputFormat && inputSource ? (
            <TitleFrame title="Load data" subtitle="Parse">
              <InputFormatStep
                inputSource={inputSource}
                initInputFormat={inputFormat}
                doneButton={false}
                onSet={({ inputFormat, signature, isArrays, timeExpression }) => {
                  setQueryString(
                    ingestQueryPatternToQuery(
                      externalConfigToIngestQueryPattern(
                        { inputSource, inputFormat, signature },
                        isArrays,
                        timeExpression,
                      ),
                    ).toString(),
                  );
                }}
                onBack={() => {
                  setExternalConfigStep({ inputSource });
                }}
              />
            </TitleFrame>
          ) : (
            <TitleFrame title="Load data" subtitle="Select input type">
              <InputSourceStep
                initInputSource={inputSource}
                onSet={(inputSource, inputFormat) => {
                  setExternalConfigStep({ inputSource, inputFormat });
                }}
              />
            </TitleFrame>
          )}
        </>
      )}
      {insertResultState.isLoading() && (
        <div className="loading-step">
          <ExecutionProgressBarPane
            execution={insertResultState.intermediate}
            onCancel={() => insertQueryManager.cancelCurrent()}
            onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
            showLiveReports={showLiveReports}
          />
          {insertResultState.intermediate?.stages && showLiveReports && (
            <ExecutionStagesPane execution={insertResultState.intermediate} />
          )}
        </div>
      )}
      {insertResultState.isError() && (
        <div className="error-step">{insertResultState.getErrorMessage()}</div>
      )}
      {insertResultState.data && (
        <div className="done-step">
          <p>Done loading data into {insertResultState.data.getInsertDatasource()}</p>
          <p>
            <Button icon={IconNames.APPLICATION}>
              {'Query '}
              <Tag minimal round>
                {insertResultState.data.getInsertDatasource()}
              </Tag>
            </Button>
          </p>
          <p>
            <Button
              icon={IconNames.RESET}
              text="Reset loader"
              onClick={() => {
                setExternalConfigStep({});
                setQueryString('');
                insertQueryManager.reset();
              }}
            />
          </p>
        </div>
      )}
    </div>
  );
});
