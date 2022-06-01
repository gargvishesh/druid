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

import { Classes, Dialog } from '@blueprintjs/core';
import React, { useState } from 'react';

import { ExternalConfig } from '../../../workbench-models';

import { InputFormatStep } from './input-format-step/input-format-step';
import { InputSourceStep } from './input-source-step/input-source-step';

import './external-config-dialog.scss';

export interface ExternalConfigDialogProps {
  initExternalConfig?: Partial<ExternalConfig>;
  onSetExternalConfig(config: ExternalConfig, isArrays: boolean[]): void;
  onClose(): void;
}

export const ExternalConfigDialog = React.memo(function ExternalConfigDialog(
  props: ExternalConfigDialogProps,
) {
  const { initExternalConfig, onClose, onSetExternalConfig } = props;

  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>(
    initExternalConfig || {},
  );

  const { inputSource, inputFormat } = externalConfigStep;

  return (
    <Dialog
      className="external-config-dialog"
      isOpen
      onClose={onClose}
      title={`Connect external data / ${inputFormat ? 'Parse' : 'Select input type'}`}
    >
      <div className={Classes.DIALOG_BODY}>
        {inputFormat && inputSource ? (
          <InputFormatStep
            inputSource={inputSource}
            initInputFormat={inputFormat}
            doneButton
            onSet={(inputFormat, columns, isArrays) => {
              onSetExternalConfig({ inputSource, inputFormat, columns }, isArrays);
              onClose();
            }}
            onBack={() => {
              setExternalConfigStep({ inputSource });
            }}
          />
        ) : (
          <InputSourceStep
            initInputSource={inputSource}
            onSet={(inputSource, inputFormat) => {
              setExternalConfigStep({ inputSource, inputFormat });
            }}
          />
        )}
      </div>
    </Dialog>
  );
});
