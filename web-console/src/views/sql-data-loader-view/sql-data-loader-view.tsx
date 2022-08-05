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

import { SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';

import {
  Execution,
  ExternalConfig,
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
  QueryWithContext,
} from '../../druid-models';
import { executionBackgroundStatusCheck, submitTaskQuery } from '../../helpers';
import { useLocalStorageState, useQueryManager } from '../../hooks';
import { LocalStorageKeys } from '../../utils';
import { ExecutionProgressBarPane } from '../workbench-view/execution-progress-bar-pane/execution-progress-bar-pane';
import { ExecutionStagesPane } from '../workbench-view/execution-stages-pane/execution-stages-pane';
import { InputFormatStep } from '../workbench-view/input-format-step/input-format-step';
import { InputSourceStep } from '../workbench-view/input-source-step/input-source-step';
import { MaxTasksButton } from '../workbench-view/max-tasks-button/max-tasks-button';

import { DoneStep } from './done-step/done-step';
import { SchemaStep } from './schema-step/schema-step';
import { TitleFrame } from './title-frame/title-frame';

import './sql-data-loader-view.scss';

export interface SqlDataLoaderViewProps {
  goToQuery(queryWithContext: QueryWithContext): void;
  goToIngestion(taskId: string): void;
}

export const SqlDataLoaderView = React.memo(function SqlDataLoaderView(
  props: SqlDataLoaderViewProps,
) {
  const { goToQuery, goToIngestion } = props;
  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>({});
  const [queryWithContext, setQueryWithContext] = useLocalStorageState<
    QueryWithContext | undefined
  >(LocalStorageKeys.SQL_DATA_LOADER_CONTENT);
  const [showLiveReports, setShowLiveReports] = useState(false);

  const { inputSource, inputFormat } = externalConfigStep;

  const [insertResultState, ingestQueryManager] = useQueryManager<
    QueryWithContext,
    Execution,
    Execution
  >({
    processQuery: async (queryWithContext: QueryWithContext, cancelToken) => {
      const ingestDatasource = SqlQuery.parse(queryWithContext.queryString)
        .getIngestTable()
        ?.getTable();

      if (!ingestDatasource) throw new Error(`Must have an ingest datasource`);

      return await submitTaskQuery({
        query: queryWithContext.queryString,
        context: queryWithContext.queryContext,
        cancelToken,
      });
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
  });

  return (
    <div className="sql-data-loader-view">
      {insertResultState.isInit() && (
        <>
          {queryWithContext ? (
            <SchemaStep
              queryString={queryWithContext.queryString}
              onQueryStringChange={queryString =>
                setQueryWithContext({ ...queryWithContext, queryString })
              }
              enableAnalyze={false}
              goToQuery={() => goToQuery(queryWithContext)}
              onBack={() => setQueryWithContext(undefined)}
              onDone={() => {
                ingestQueryManager.runQuery(queryWithContext);
              }}
              extraCallout={
                <MaxTasksButton
                  queryContext={queryWithContext.queryContext || {}}
                  changeQueryContext={queryContext =>
                    setQueryWithContext({ ...queryWithContext, queryContext })
                  }
                  minimal
                />
              }
            />
          ) : inputFormat && inputSource ? (
            <TitleFrame title="Load data" subtitle="Parse">
              <InputFormatStep
                inputSource={inputSource}
                initInputFormat={inputFormat}
                doneButton={false}
                onSet={({ inputFormat, signature, isArrays, timeExpression }) => {
                  setQueryWithContext({
                    queryString: ingestQueryPatternToQuery(
                      externalConfigToIngestQueryPattern(
                        { inputSource, inputFormat, signature },
                        isArrays,
                        timeExpression,
                      ),
                    ).toString(),
                    queryContext: {
                      finalizeAggregations: false,
                      groupByEnableMultiValueUnnesting: false,
                    },
                  });
                }}
                altText="Skip the wizard and continue with custom SQL"
                onAltSet={({ inputFormat, signature, isArrays, timeExpression }) => {
                  goToQuery({
                    queryString: ingestQueryPatternToQuery(
                      externalConfigToIngestQueryPattern(
                        { inputSource, inputFormat, signature },
                        isArrays,
                        timeExpression,
                      ),
                    ).toString(),
                  });
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
                mode="sampler"
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
            onCancel={() => ingestQueryManager.cancelCurrent()}
            onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
            showLiveReports={showLiveReports}
          />
          {insertResultState.intermediate?.stages && showLiveReports && (
            <ExecutionStagesPane
              execution={insertResultState.intermediate}
              goToIngestion={goToIngestion}
            />
          )}
        </div>
      )}
      {insertResultState.isError() && (
        <div className="error-step">{insertResultState.getErrorMessage()}</div>
      )}
      {insertResultState.data && (
        <DoneStep
          execution={insertResultState.data}
          goToQuery={goToQuery}
          onReset={() => {
            setExternalConfigStep({});
            setQueryWithContext(undefined);
            ingestQueryManager.reset();
          }}
        />
      )}
    </div>
  );
});
