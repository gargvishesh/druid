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

import { formatDurationHybrid, pluralIfNeeded } from '../../../utils';
import { Execution } from '../../../workbench-models';

import './execution-summary-button.scss';

export interface ExecutionSummaryButtonProps {
  execution: Execution | undefined;
  onExecutionDetail(): void;
  onReset?: () => void;
}

export const ExecutionSummaryButton = React.memo(function ExecutionSummaryButton(
  props: ExecutionSummaryButtonProps,
) {
  const { execution, onExecutionDetail, onReset } = props;
  const queryResult = execution?.result;

  const buttons: JSX.Element[] = [];

  if (queryResult) {
    const wrapQueryLimit = queryResult.getSqlOuterLimit();
    const hasMoreResults = queryResult.getNumResults() === wrapQueryLimit;

    const resultCount = hasMoreResults
      ? `${queryResult.getNumResults() - 1}+ results`
      : pluralIfNeeded(queryResult.getNumResults(), 'result');

    const warningCount = execution?.stages?.getWarningCount();

    buttons.push(
      <Button
        key="results"
        minimal
        text={
          resultCount +
          (warningCount ? ` and ${pluralIfNeeded(warningCount, 'warning')}` : '') +
          (execution.duration ? ` in ${formatDurationHybrid(execution.duration)}` : '')
        }
        rightIcon={IconNames.INFO_SIGN}
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
    <ButtonGroup className="execution-summary-button" minimal>
      {buttons}
    </ButtonGroup>
  );
});
