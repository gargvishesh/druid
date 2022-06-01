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
import { Execution } from '../../../workbench-models';
import { ExecutionDetailsPane } from '../execution-details-pane/execution-details-pane';
import { getTaskExecution } from '../execution-utils';

export interface ExecutionDetailsPaneLoaderProps {
  id: string;
}

export const ExecutionDetailsPaneLoader = React.memo(function ExecutionDetailsPaneLoader(
  props: ExecutionDetailsPaneLoaderProps,
) {
  const { id } = props;

  const [executionState, queryManager] = useQueryManager<string, Execution>({
    processQuery: (id: string) => {
      return getTaskExecution(id);
    },
    initQuery: id,
  });

  useInterval(() => {
    const execution = executionState.data;
    if (!execution) return;
    if (execution.isWaitingForQuery()) {
      queryManager.rerunLastQuery(true);
    }
  }, 1000);

  const execution = executionState.getSomeData();
  if (execution) {
    return <ExecutionDetailsPane execution={execution} />;
  } else if (executionState.isLoading()) {
    return <Loader className="execution-stages-pane" />;
  } else if (executionState.isError()) {
    return <div>{executionState.getErrorMessage()}</div>;
  } else {
    return null;
  }
});
