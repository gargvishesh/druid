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

import { Button, Classes, Dialog, Intent, NumericInput } from '@blueprintjs/core';
import React, { useState } from 'react';

const MIN = 1;

interface NumericInputDialogProps {
  title: string;
  initValue: number;
  onSave(value: number): void;
  onClose(): void;
}

export const NumericInputDialog = React.memo(function NumericInputDialog(
  props: NumericInputDialogProps,
) {
  const { title, initValue, onSave, onClose } = props;

  const [value, setValue] = useState<number>(initValue);

  return (
    <Dialog
      className="numeric-input-dialog"
      onClose={onClose}
      isOpen
      title={title}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <NumericInput
          value={value}
          onValueChange={(v: number) => {
            if (isNaN(v)) return;
            setValue(Math.max(v, MIN));
          }}
          min={MIN}
          stepSize={1}
          minorStepSize={null}
          majorStepSize={10}
          fill
          autoFocus
        />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="OK"
            intent={Intent.PRIMARY}
            onClick={() => {
              onSave(value);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
