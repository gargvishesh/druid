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
import React, { useState } from 'react';

import { useInterval } from '../../../hooks';
import { formatDurationHybrid } from '../../../utils';
import { Execution } from '../../../workbench-models';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';

import './execution-timer.scss';

export interface ExecutionTimerProps {
  execution: Execution | undefined;
  onCancel(): void;
}

export const ExecutionTimer = React.memo(function ExecutionTimer(props: ExecutionTimerProps) {
  const { execution, onCancel } = props;
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [currentTime, setCurrentTime] = useState(Date.now());

  useInterval(() => {
    setCurrentTime(Date.now());
  }, 25);

  function cancelMaybeConfirm() {
    if (execution?.isProcessingData()) {
      setShowCancelConfirm(true);
    } else {
      onCancel();
    }
  }

  const elapsed = execution?.startTime ? currentTime - execution.startTime.valueOf() : 0;
  if (elapsed <= 0) return null;
  return (
    <ButtonGroup className="execution-timer">
      <Button
        className="timer"
        icon={IconNames.STOPWATCH}
        text={formatDurationHybrid(elapsed)}
        minimal
      />
      <Button icon={IconNames.CROSS} minimal onClick={cancelMaybeConfirm} />
      {showCancelConfirm && (
        <CancelQueryDialog onCancel={onCancel} onDismiss={() => setShowCancelConfirm(false)} />
      )}
    </ButtonGroup>
  );
});
