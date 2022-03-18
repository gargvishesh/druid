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

import { Button, Classes, Dialog, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';
import { v4 as uuidv4 } from 'uuid';

import { UserEntry } from '../user-management-models';

import './user-editor-dialog.scss';

interface UserEditorDialogProps {
  initUser: UserEntry | undefined;
  onSave(username: string, password: string): void;
  onClose(): void;
}

export const UserEditorDialog = React.memo(function UserEditorDialog(props: UserEditorDialogProps) {
  const { initUser, onSave, onClose } = props;

  const [username, setUsername] = useState<string>(initUser?.username || '');
  const [password, setPassword] = useState<string>('');

  return (
    <Dialog
      className="user-editor-dialog"
      onClose={onClose}
      isOpen
      title={initUser ? `Set password for '${initUser.username}'` : 'Create user'}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        {!initUser && (
          <FormGroup label="Username">
            <InputGroup value={username} onChange={e => setUsername(e.target.value)} autoFocus />
          </FormGroup>
        )}
        <FormGroup label="Password">
          <InputGroup
            value={password}
            onChange={e => setPassword(e.target.value)}
            placeholder={initUser ? undefined : 'Leave blank to not set'}
            rightElement={<Button text="Generate UUID" onClick={() => setPassword(uuidv4())} />}
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={initUser ? !password : !username}
            onClick={() => {
              if (!username) return;
              onSave(username, password);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
