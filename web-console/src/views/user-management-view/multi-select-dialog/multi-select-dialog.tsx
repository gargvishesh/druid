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

import { Button, Classes, Dialog, FormGroup, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { ArrayInput } from '../../../components';

import './multi-select-dialog.scss';

interface MultiSelectDialogProps {
  title: string;
  initSelection: string[];
  suggestions: string[];
  onSave(newSelection: string[]): void;
  onClose(): void;
}

export const MultiSelectDialog = React.memo(function MultiSelectDialog(
  props: MultiSelectDialogProps,
) {
  const { title, initSelection, suggestions, onSave, onClose } = props;

  const [selection, setSelection] = useState<string[]>(initSelection);

  return (
    <Dialog
      className="multi-select-dialog"
      onClose={onClose}
      isOpen
      title={title}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <FormGroup>
          <ArrayInput
            values={selection}
            onChange={v => setSelection(v || [])}
            suggestions={suggestions}
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            onClick={() => {
              onSave(selection);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
