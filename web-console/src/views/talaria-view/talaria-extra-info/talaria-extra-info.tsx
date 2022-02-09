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

import { Tooltip2 } from '@blueprintjs/popover2';
import React from 'react';

import { QueryExecution } from '../../../talaria-models';
import { pluralIfNeeded } from '../../../utils';

import './talaria-extra-info.scss';

export interface TalariaExtraInfoProps {
  queryExecution: QueryExecution;
  onStats(): void;
}

export const TalariaExtraInfo = React.memo(function TalariaExtraInfo(props: TalariaExtraInfoProps) {
  const { queryExecution, onStats } = props;
  const queryResult = queryExecution.result;
  if (!queryResult) return null;

  const wrapQueryLimit = queryResult.getSqlOuterLimit();
  const hasMoreResults = queryResult.getNumResults() === wrapQueryLimit;

  const resultCount = hasMoreResults
    ? `${queryResult.getNumResults() - 1}+ results`
    : pluralIfNeeded(queryResult.getNumResults(), 'result');

  let tooltipContent: JSX.Element | undefined;
  const taskId = queryResult.resultContext?.taskId;
  if (taskId) {
    tooltipContent = (
      <>
        Task ID: <strong>{taskId}</strong>
      </>
    ); // (click to copy)
  }

  return (
    <div className="talaria-extra-info" onClick={onStats}>
      <Tooltip2 content={tooltipContent} hoverOpenDelay={500} placement="top-start">
        {resultCount +
          (queryExecution.duration ? ` in ${(queryExecution.duration / 1000).toFixed(2)}s` : '')}
      </Tooltip2>
    </div>
  );
});
