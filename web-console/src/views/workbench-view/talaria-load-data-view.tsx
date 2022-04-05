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
import {
  ExternalConfig,
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
  QueryExecution,
} from '../../talaria-models';
import { getContextFromSqlQuery, LocalStorageKeys } from '../../utils';

import { executionBackgroundStatusCheck, submitAsyncQuery } from './execution-utils';
import { InputFormatStep } from './external-config-dialog/input-format-step/input-format-step';
import { InputSourceStep } from './external-config-dialog/input-source-step/input-source-step';
import { SchemaStep } from './schema-step/schema-step';
import { StageProgress } from './stage-progress/stage-progress';
import { TalariaStats } from './talaria-stats/talaria-stats';
import { TitleFrame } from './title-frame/title-frame';

import './talaria-load-data-view.scss';

export interface TalariaLoadDataViewProps {
  goToQuery: (initSql: string) => void;
}

export const TalariaLoadDataView = React.memo(function TalariaLoadDataView(
  props: TalariaLoadDataViewProps,
) {
  const { goToQuery } = props;
  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>({});
  const [queryString, setQueryString] = useLocalStorageState<string>(
    LocalStorageKeys.SQLOADER_QUERY,
    '',
  );
  const [showLiveReports, setShowLiveReports] = useState(false);

  const { inputSource, inputFormat } = externalConfigStep;

  const [insertResultState, insertQueryManager] = useQueryManager<
    string,
    QueryExecution,
    QueryExecution
  >({
    processQuery: async (queryString: string, cancelToken) => {
      const insertDatasource = SqlQuery.parse(queryString).getInsertIntoTable()?.getTable();
      if (!insertDatasource) throw new Error(`Must have insert datasource`);

      return await submitAsyncQuery({
        query: queryString,
        context: {
          ...getContextFromSqlQuery(queryString),
          talaria: true,
        },
        cancelToken,
      });
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
  });

  return (
    <div className="talaria-load-data-view">
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
                onSet={(inputFormat, columns, isArrays) => {
                  setQueryString(
                    ingestQueryPatternToQuery(
                      externalConfigToIngestQueryPattern(
                        { inputSource, inputFormat, columns },
                        isArrays,
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
          <StageProgress
            queryExecution={insertResultState.intermediate}
            onCancel={() => insertQueryManager.cancelCurrent()}
            onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
            showLiveReports={showLiveReports}
          />
          {insertResultState.intermediate?.stages && showLiveReports && (
            <TalariaStats stages={insertResultState.intermediate?.stages} />
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
