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

import React from 'react';

import { Loader } from '../../../components';
import { useInterval, useQueryManager } from '../../../hooks';
import { QueryExecution } from '../../../talaria-models';
import { getAsyncExecution } from '../execution-utils';
import { TalariaStats } from '../talaria-stats/talaria-stats';

export interface TalariaStatsLoaderProps {
  id: string;
}

export const TalariaStatsLoader = React.memo(function TalariaStatsLoader(
  props: TalariaStatsLoaderProps,
) {
  const { id } = props;

  const [reportState, queryManager] = useQueryManager<string, QueryExecution>({
    processQuery: (taskId: string) => {
      return getAsyncExecution(taskId);
    },
    initQuery: id,
  });

  useInterval(() => {
    const report = reportState.data;
    if (!report) return;
    if (report.isWaitingForQuery()) {
      queryManager.rerunLastQuery(true);
    }
  }, 1000);

  const report = reportState.getSomeData();
  if (report) {
    return <TalariaStats stages={report.stages || []} error={report.error} />;
  } else if (reportState.isLoading()) {
    return <Loader className="talaria-stats" />;
  } else if (reportState.isError()) {
    return <div>{reportState.getErrorMessage()}</div>;
  } else {
    return null;
  }
});
