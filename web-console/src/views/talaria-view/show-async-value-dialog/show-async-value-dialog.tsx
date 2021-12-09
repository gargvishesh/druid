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

import { Button, Classes, Dialog, Intent, TextArea } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import copy from 'copy-to-clipboard';
import React from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { AppToaster } from '../../../singletons';

import './show-async-value-dialog.scss';

export interface ShowAsyncValueDialogProps {
  title: string;
  loadValue: () => Promise<string>;
  onClose: () => void;
}

export const ShowAsyncValueDialog = React.memo(function ShowAsyncValueDialog(
  props: ShowAsyncValueDialogProps,
) {
  const { title, onClose, loadValue } = props;

  const [valueState] = useQueryManager({
    processQuery: () => {
      return loadValue();
    },
    initQuery: '',
  });

  const value = valueState.data;

  function handleCopy() {
    if (!value) return;
    copy(value, { format: 'text/plain' });
    AppToaster.show({
      message: 'Value copied to clipboard',
      intent: Intent.SUCCESS,
    });
  }

  return (
    <Dialog className="show-async-value-dialog" isOpen onClose={onClose} title={title}>
      {value && <TextArea value={value} />}
      {valueState.isLoading() && <Loader />}
      {valueState.isError() && <div>{valueState.getErrorMessage()}</div>}
      <div className={Classes.DIALOG_FOOTER_ACTIONS}>
        <Button icon={IconNames.DUPLICATE} text="Copy" onClick={handleCopy} />
        <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
      </div>
    </Dialog>
  );
});
