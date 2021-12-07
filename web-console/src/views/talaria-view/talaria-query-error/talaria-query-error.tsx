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
import { TalariaTaskError } from '../../../talaria-models';

import './talaria-query-error.scss';

const ClickToCopy = React.memo(function ClickToCopy(props: { text: string }) {
  const { text } = props;
  return (
    <span
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
    </span>
  );
});

export interface TalariaQueryErrorProps {
  taskError: TalariaTaskError;
}

export const TalariaQueryError = React.memo(function TalariaQueryError(
  props: TalariaQueryErrorProps,
) {
  const { taskError } = props;
  const [stackToShow, setStackToShow] = useState<string | undefined>();

  const { error, exceptionStackTrace, taskId, host } = taskError;

  return (
    <div className="talaria-query-error">
      <p>
        {error.errorCode && <>{`${error.errorCode}: `}</>}
        {error.errorMessage || (exceptionStackTrace || '').split('\n')[0]}
        {exceptionStackTrace && (
          <>
            {' '}
            <span
              className="stack-trace"
              onClick={() => {
                setStackToShow(exceptionStackTrace);
              }}
            >
              (Stack trace)
            </span>
          </>
        )}
      </p>
      {taskId && (
        <p>
          Failed task ID: <ClickToCopy text={taskId} />
        </p>
      )}
      {host && (
        <p>
          On host: <ClickToCopy text={host} />
        </p>
      )}
      {stackToShow && (
        <ShowValueDialog size="large" onClose={() => setStackToShow(undefined)} str={stackToShow} />
      )}
    </div>
  );
});
