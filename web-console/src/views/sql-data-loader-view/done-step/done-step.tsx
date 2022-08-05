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

import { Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlTableRef } from 'druid-query-toolkit';
import React from 'react';

import { Execution, QueryWithContext } from '../../../druid-models';

export interface DoneStepProps {
  execution: Execution;
  goToQuery(queryWithContext: QueryWithContext): void;
  onReset(): void;
}

export const DoneStep = function DoneStep(props: DoneStepProps) {
  const { execution, goToQuery, onReset } = props;

  return (
    <div className="done-step">
      <p>Done loading data into {execution.getIngestDatasource()}</p>
      <p>
        <Button
          icon={IconNames.APPLICATION}
          text={`Query: ${execution.getIngestDatasource()}`}
          onClick={() => {
            onReset();
            goToQuery({
              queryString: `SELECT * FROM ${SqlTableRef.create(execution.getIngestDatasource()!)}`,
            });
          }}
        />
      </p>
      <p>
        <Button icon={IconNames.RESET} text="Reset loader" onClick={onReset} />
      </p>
    </div>
  );
};
