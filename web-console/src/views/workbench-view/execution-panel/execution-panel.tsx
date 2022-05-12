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

import { Execution } from '../../../talaria-models';
import { InsertSuccess } from '../insert-success/insert-success';
import { QueryOutput2 } from '../query-output2/query-output2';
import { TalariaQueryError } from '../talaria-query-error/talaria-query-error';
import { TalariaStats } from '../talaria-stats/talaria-stats';

export interface ExecutionPanelProps {
  execution: Execution;
}

export const ExecutionPanel = React.memo(function ExecutionPanel(props: ExecutionPanelProps) {
  const { execution } = props;

  const handleExport = () => {};
  const handleQueryAction = () => {};
  const handleQueryStringChange = () => {};

  return (
    <div className="execution-panel">
      {execution.result ? (
        <QueryOutput2
          runeMode={false}
          queryResult={execution.result}
          onExport={handleExport}
          onQueryAction={handleQueryAction}
        />
      ) : execution.isSuccessfulInsert() ? (
        <InsertSuccess
          insertQueryExecution={execution}
          onStats={() => {
            console.log('ToDo');
          }}
          onQueryChange={handleQueryStringChange}
        />
      ) : execution.error ? (
        <div className="stats-container">
          <TalariaQueryError execution={execution} />
          {execution.stages && <TalariaStats stages={execution.stages} error={execution.error} />}
        </div>
      ) : (
        <div>Unknown query execution state</div>
      )}
    </div>
  );
});
