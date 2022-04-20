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

import {
  Button,
  Classes,
  ControlGroup,
  Dialog,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { deepSet } from '../../../utils';
import {
  ALL_PERMISSIONS,
  Permission,
  PERMISSION_TYPES,
  RoleEntry,
} from '../user-management-models';

import './role-editor-dialog.scss';

interface RoleEditorDialogProps {
  initRole: RoleEntry | undefined;
  onSave(roleName: string, permissions: Permission[]): void;
  onClose(): void;
}

export const RoleEditorDialog = React.memo(function RoleEditorDialog(props: RoleEditorDialogProps) {
  const { initRole, onSave, onClose } = props;

  const [name, setName] = useState<string>(initRole?.name || '');
  const [permissions, setPermissions] = useState<Permission[]>(
    initRole?.permissions || ALL_PERMISSIONS,
  );

  return (
    <Dialog
      className="role-editor-dialog"
      onClose={onClose}
      isOpen
      title={initRole ? `Edit permissions for '${initRole.name}'` : 'Create role'}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        {!initRole && (
          <FormGroup label="Role name">
            <InputGroup value={name} onChange={e => setName(e.target.value)} autoFocus />
          </FormGroup>
        )}
        {!permissions.length && <FormGroup>No permissions</FormGroup>}
        {permissions.map((permission, index) => (
          <FormGroup key={index}>
            <ControlGroup fill>
              <HTMLSelect
                value={permission.action}
                onChange={(e: any) =>
                  setPermissions(
                    permissions.map((p, i) => (i === index ? { ...p, action: e.target.value } : p)),
                  )
                }
              >
                <option value="READ">READ</option>
                <option value="WRITE">WRITE</option>
              </HTMLSelect>
              <HTMLSelect
                value={permission.resource.type}
                onChange={(e: any) =>
                  setPermissions(
                    permissions.map((p, i) =>
                      i === index ? deepSet(p, 'resource.type', e.target.value) : p,
                    ),
                  )
                }
              >
                {PERMISSION_TYPES.map(p => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </HTMLSelect>
              <InputGroup
                value={permission.resource.name}
                onChange={e =>
                  setPermissions(
                    permissions.map((p, i) =>
                      i === index ? deepSet(p, 'resource.name', e.target.value) : p,
                    ),
                  )
                }
              />
              <Button
                icon={IconNames.CROSS}
                onClick={() => setPermissions(permissions.filter((_, i) => i !== index))}
              />
            </ControlGroup>
          </FormGroup>
        ))}

        <FormGroup>
          <Button
            icon={IconNames.PLUS}
            text="Add permission"
            onClick={() =>
              setPermissions(
                permissions.concat({
                  action: 'READ',
                  resource: { type: 'DATASOURCE', name: '.*' },
                }),
              )
            }
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={!name}
            onClick={() => {
              if (!name) return;
              onSave(name, permissions);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
