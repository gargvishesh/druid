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

import React, { useState } from 'react';

import {
  ExternalConfig,
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
  QueryWithContext,
} from '../../druid-models';
import { useLocalStorageState } from '../../hooks';
import { LocalStorageKeys } from '../../utils';
import { InputFormatStep } from '../workbench-view/input-format-step/input-format-step';
import { InputSourceStep } from '../workbench-view/input-source-step/input-source-step';
import { MaxTasksButton } from '../workbench-view/max-tasks-button/max-tasks-button';

import { IngestionProgressDialog } from './ingestion-progress-dialog/ingestion-progress-dialog';
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
  const [runningQueryWithContext, setRunningQueryWithContext] = useState<
    QueryWithContext | undefined
  >();

  const { inputSource, inputFormat } = externalConfigStep;

  return (
    <div className="sql-data-loader-view">
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
            setRunningQueryWithContext(queryWithContext);
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
      {runningQueryWithContext && (
        <IngestionProgressDialog
          queryWithContext={runningQueryWithContext}
          goToQuery={goToQuery}
          goToIngestion={goToIngestion}
          onClose={() => setRunningQueryWithContext(undefined)}
        />
      )}
    </div>
  );
});
