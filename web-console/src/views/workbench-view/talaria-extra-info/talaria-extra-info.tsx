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

import { Button, ButtonGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { Execution } from '../../../talaria-models';
import { pluralIfNeeded } from '../../../utils';

import './talaria-extra-info.scss';

export interface TalariaExtraInfoProps {
  execution: Execution | undefined;
  onExecutionDetail(): void;
  onReset?: () => void;
}

export const TalariaExtraInfo = React.memo(function TalariaExtraInfo(props: TalariaExtraInfoProps) {
  const { execution, onExecutionDetail, onReset } = props;
  const queryResult = execution?.result;

  const buttons: JSX.Element[] = [];

  if (queryResult) {
    const wrapQueryLimit = queryResult.getSqlOuterLimit();
    const hasMoreResults = queryResult.getNumResults() === wrapQueryLimit;

    const resultCount = hasMoreResults
      ? `${queryResult.getNumResults() - 1}+ results`
      : pluralIfNeeded(queryResult.getNumResults(), 'result');

    buttons.push(
      <Button
        key="results"
        minimal
        text={
          resultCount + (execution.duration ? ` in ${(execution.duration / 1000).toFixed(2)}s` : '')
        }
        onClick={() => {
          if (!execution) return;
          if (execution.engine === 'sql-task') {
            onExecutionDetail();
          }
        }}
      />,
    );
  }

  if (onReset) {
    buttons.push(<Button key="reset" icon={IconNames.CROSS} minimal onClick={onReset} />);
  }

  return (
    <ButtonGroup className="talaria-extra-info" minimal>
      {buttons}
    </ButtonGroup>
  );
});
