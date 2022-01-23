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
import { getContextFromSqlQuery, IntermediateQueryState } from '../../utils';

import { InitStep } from './external-config-dialog/init-step/init-step';
import { InputFormatStep } from './external-config-dialog/input-format-step/input-format-step';
import { InputSourceStep } from './external-config-dialog/input-source-step/input-source-step';
import { SchemaStep } from './schema-step/schema-step';
import { StageProgress } from './stage-progress/stage-progress';
import { TalariaStats } from './talaria-stats/talaria-stats';
import {
  cancelAsyncQueryOnCancel,
  submitAsyncQuery,
  talariaBackgroundStatusCheck,
} from './talaria-utils';

import './talaria-load-data-view.scss';

export interface TalariaLoadDataViewProps {
  goToQuery: (initSql: string) => void;
}

export const TalariaLoadDataView = React.memo(function TalariaLoadDataView(
  props: TalariaLoadDataViewProps,
) {
  const { goToQuery } = props;
  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>({});
  const [queryString, setQueryString] = useLocalStorageState<string>('loader-query', '');
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

      const summary = await submitAsyncQuery({
        query: queryString,
        context: {
          ...getContextFromSqlQuery(queryString),
          talaria: true,
        },
        cancelToken,
      });

      cancelAsyncQueryOnCancel(summary.id, cancelToken);
      return new IntermediateQueryState(summary);
    },
    backgroundStatusCheck: talariaBackgroundStatusCheck,
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
          ) : !inputSource ? (
            <InitStep
              onSet={inputSource => {
                setExternalConfigStep({ inputSource });
              }}
            />
          ) : !inputFormat ? (
            <InputSourceStep
              initInputSource={inputSource}
              onSet={(inputSource, inputFormat) => {
                setExternalConfigStep({ inputSource, inputFormat });
              }}
              onBack={() => {
                setExternalConfigStep({});
              }}
            />
          ) : (
            <InputFormatStep
              inputSource={inputSource}
              initInputFormat={inputFormat}
              doneButton={false}
              onSet={(inputFormat, columns) => {
                setQueryString(
                  ingestQueryPatternToQuery(
                    externalConfigToIngestQueryPattern({ inputSource, inputFormat, columns }),
                  ).toString(),
                );
              }}
              onBack={() => {
                setExternalConfigStep({ inputSource });
              }}
            />
          )}
        </>
      )}
      {insertResultState.isLoading() && (
        <div className="loading-step">
          <StageProgress
            stages={insertResultState.intermediate?.stages}
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
          <p>Done loading data into {insertResultState.data.getInsertDataSource()}</p>
          <p>
            <Button icon={IconNames.APPLICATION}>
              {'Query '}
              <Tag minimal round>
                {insertResultState.data.getInsertDataSource()}
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
