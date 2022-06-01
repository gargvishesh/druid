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

import { Intent } from '@blueprintjs/core';
import copy from 'copy-to-clipboard';
import React, { useState } from 'react';

import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import { AppToaster } from '../../../singletons';
import { downloadQueryProfile } from '../../../utils';
import { Execution } from '../../../workbench-models';

import './execution-error-pane.scss';

const ClickToCopy = React.memo(function ClickToCopy(props: { text: string }) {
  const { text } = props;
  return (
    <a
      className="click-to-copy"
      title={`Click to copy:\n${text}`}
      onClick={() => {
        copy(text, { format: 'text/plain' });
        AppToaster.show({
          message: `${text} copied to clipboard`,
          intent: Intent.SUCCESS,
        });
      }}
    >
      {text}
    </a>
  );
});

export interface ExecutionErrorPaneProps {
  execution: Execution;
}

export const ExecutionErrorPane = React.memo(function ExecutionErrorPane(
  props: ExecutionErrorPaneProps,
) {
  const { execution } = props;
  const [stackToShow, setStackToShow] = useState<string | undefined>();
  if (!execution.error) return null;

  const { error, exceptionStackTrace, taskId, host } = execution.error;

  return (
    <div className="execution-error-pane">
      <p className="error-message-text">
        {error.errorCode && <>{`${error.errorCode}: `}</>}
        {error.errorMessage || (exceptionStackTrace || '').split('\n')[0]}
        {exceptionStackTrace && (
          <>
            {' '}
            <a
              onClick={() => {
                setStackToShow(exceptionStackTrace);
              }}
            >
              (Stack trace)
            </a>
          </>
        )}
      </p>
      <p>
        {taskId && (
          <>
            Failed task ID: <ClickToCopy text={taskId} />
            &nbsp;
          </>
        )}
        {host && (
          <>
            On host: <ClickToCopy text={host} />
            &nbsp;
          </>
        )}
        Debug:{' '}
        <a
          onClick={() => {
            void downloadQueryProfile(execution.id);
          }}
        >
          download query profile
        </a>
      </p>

      {stackToShow && (
        <ShowValueDialog
          size="large"
          title="Full stack trace"
          onClose={() => setStackToShow(undefined)}
          str={stackToShow}
        />
      )}
    </div>
  );
});
