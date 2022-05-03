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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  RefreshButton,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { useQueryManager } from '../../hooks';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import { deepGet, LocalStorageKeys } from '../../utils';

import { MultiSelectDialog } from './multi-select-dialog/multi-select-dialog';
import { RoleEditorDialog } from './role-editor-dialog/role-editor-dialog';
import { UserEditorDialog } from './user-editor-dialog/user-editor-dialog';
import { formatPermission, Permission, RoleEntry, UserEntry } from './user-management-models';

import './users-and-roles.scss';

function getErrorMessage(e: any): string {
  return deepGet(e, 'response.data.error') || e.message;
}

function execSwallowAsyncError<T>(fn: () => Promise<T>): Promise<T | void> {
  return fn().catch(() => {});
}

export interface UsersAndRolesProps {
  authenticatorName: string;
  authorizerName: string;
}

export const UsersAndRoles = function UsersAndRoles(props: UsersAndRolesProps) {
  const { authorizerName, authenticatorName } = props;

  const [openUserEditorOn, setOpenUserEditorOn] = useState<
    { user: UserEntry | undefined } | undefined
  >();
  const [openRoleEditorOn, setOpenRoleEditorOn] = useState<
    { role: RoleEntry | undefined } | undefined
  >();
  const [assignRolesForUser, setAssignRolesForUser] = useState<UserEntry | undefined>();
  const [assignUsersForRole, setAssignUsersForRole] = useState<RoleEntry | undefined>();
  const [userToDelete, setUserToDelete] = useState<UserEntry | undefined>();
  const [roleToDelete, setRoleToDelete] = useState<RoleEntry | undefined>();

  const [userState, userQueryManager] = useQueryManager<string, UserEntry[]>({
    initQuery: 'x',
    processQuery: async () => {
      const userListResp = await Api.instance.get(
        `/proxy/coordinator/druid-ext/basic-security/authentication/db/${Api.encodePath(
          authenticatorName,
        )}/users`,
      );

      const users: UserEntry[] = userListResp.data.map((username: string) => ({ username }));

      for (const user of users) {
        await Promise.all([
          execSwallowAsyncError(async () => {
            const detailResp = await Api.instance.get(
              `/proxy/coordinator/druid-ext/basic-security/authentication/db/${Api.encodePath(
                authenticatorName,
              )}/users/${Api.encodePath(user.username)}?full&simplifyPermissions`,
            );

            user.credentials = Boolean(detailResp.data.credentials);
          }),
          execSwallowAsyncError(async () => {
            const detailResp = await Api.instance.get(
              `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                authorizerName,
              )}/users/${Api.encodePath(user.username)}?simplifyPermissions`,
            );

            user.roles = detailResp.data.roles;
          }),
        ]);
      }

      return users;
    },
  });

  const [roleState, roleQueryManager] = useQueryManager<string, RoleEntry[]>({
    initQuery: 'x',
    processQuery: async () => {
      const roleListResp = await Api.instance.get(
        `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
          authorizerName,
        )}/roles`,
      );

      const roles: RoleEntry[] = roleListResp.data.map((name: string) => ({ name }));

      for (const role of roles) {
        try {
          const detailResp = await Api.instance.get(
            `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
              authorizerName,
            )}/roles/${Api.encodePath(role.name)}?full&simplifyPermissions`,
          );

          role.users = detailResp.data.users;
          role.permissions = detailResp.data.permissions;
        } catch {}
      }

      return roles;
    },
  });

  const users = userState.data || [];
  const roles = roleState.data || [];

  return (
    <div className="users-and-roles">
      <div className="user-panel">
        <ViewControlBar label="Users">
          <Button
            icon={IconNames.PLUS}
            text="Add user"
            onClick={() => setOpenUserEditorOn({ user: undefined })}
          />
          <RefreshButton
            onRefresh={auto => userQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.USERS_REFRESH_RATE}
          />
        </ViewControlBar>
        <ReactTable
          data={users}
          loading={userState.loading}
          noDataText={userState.isEmpty() ? 'No users' : userState.getErrorMessage() || ''}
          filterable
          defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
          pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
          showPagination={users.length > STANDARD_TABLE_PAGE_SIZE}
          columns={[
            {
              Header: 'User',
              accessor: 'username',
              width: 160,
              className: 'padded',
            },
            {
              Header: 'Assigned roles',
              accessor: 'roles',
              width: 240,
              filterable: false,
              className: 'padded',
              Cell({ value }) {
                if (!value) return null;
                return (
                  <>
                    {value.map((v: string, i: number) => (
                      <div key={i}>{v}</div>
                    ))}
                  </>
                );
              },
            },
            {
              Header: 'Status',
              id: 'credentials',
              className: 'padded',
              accessor: d => {
                const issues: string[] = [];
                if (!d.credentials) issues.push('No credentials');
                if (!d.roles) issues.push('Not in authorization system');
                return issues.join(', ') || 'OK';
              },
              width: 200,
            },
            {
              Header: ACTION_COLUMN_LABEL,
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              accessor: 'username',
              filterable: false,
              Cell: ({ original }) => (
                <ActionCell
                  actions={[
                    {
                      icon: IconNames.EDIT,
                      title: 'Set password',
                      onAction: () => setOpenUserEditorOn({ user: original }),
                    },
                    {
                      icon: IconNames.PEOPLE,
                      title: 'Assign roles',
                      onAction: () => setAssignRolesForUser(original),
                    },
                    {
                      icon: IconNames.TRASH,
                      title: 'Delete user',
                      intent: Intent.DANGER,
                      onAction: () => setUserToDelete(original),
                    },
                  ]}
                />
              ),
            },
          ]}
        />
      </div>
      <div className="role-panel">
        <ViewControlBar label="Roles">
          <Button
            icon={IconNames.PLUS}
            text="Add role"
            onClick={() => setOpenRoleEditorOn({ role: undefined })}
          />
          <RefreshButton
            onRefresh={auto => roleQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.ROLES_REFRESH_RATE}
          />
        </ViewControlBar>
        <ReactTable
          data={roles}
          loading={roleState.loading}
          noDataText={roleState.isEmpty() ? 'No roles' : roleState.getErrorMessage() || ''}
          filterable
          defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
          pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
          showPagination={roles.length > STANDARD_TABLE_PAGE_SIZE}
          columns={[
            {
              Header: 'Role',
              accessor: 'name',
              width: 140,
              className: 'padded',
            },
            {
              Header: 'Assigned users',
              accessor: 'users',
              width: 140,
              className: 'padded',
              filterable: false,
              Cell({ value }) {
                if (!value) return null;
                return (
                  <>
                    {value.map((v: string, i: number) => (
                      <div key={i}>{v}</div>
                    ))}
                  </>
                );
              },
            },
            {
              Header: 'Permissions',
              accessor: 'permissions',
              filterable: false,
              width: 300,
              className: 'padded',
              Cell({ value }) {
                return (
                  <>
                    {value.map((v: Permission, i: number) => (
                      <div key={i}>{formatPermission(v)}</div>
                    ))}
                  </>
                );
              },
            },
            {
              Header: ACTION_COLUMN_LABEL,
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              accessor: 'name',
              filterable: false,
              Cell: ({ original }) => (
                <ActionCell
                  actions={[
                    {
                      icon: IconNames.EDIT,
                      title: 'Edit permissions',
                      onAction: () => setOpenRoleEditorOn({ role: original }),
                    },
                    {
                      icon: IconNames.PERSON,
                      title: 'Assign users',
                      onAction: () => setAssignUsersForRole(original),
                    },
                    {
                      icon: IconNames.TRASH,
                      title: 'Delete role',
                      intent: Intent.DANGER,
                      onAction: () => setRoleToDelete(original),
                    },
                  ]}
                />
              ),
            },
          ]}
        />
      </div>
      {openUserEditorOn && (
        <UserEditorDialog
          initUser={openUserEditorOn.user}
          onSave={async (username, password) => {
            const user = openUserEditorOn?.user;
            if (!user) {
              try {
                await Api.instance.post(
                  `/proxy/coordinator/druid-ext/basic-security/authentication/db/${Api.encodePath(
                    authenticatorName,
                  )}/users/${Api.encodePath(username)}`,
                  {},
                );
              } catch (e) {
                AppToaster.show({
                  intent: Intent.DANGER,
                  message: `Could not create user '${username}' in the authentication system: ${getErrorMessage(
                    e,
                  )}`,
                });
                return;
              }

              try {
                await Api.instance.post(
                  `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                    authorizerName,
                  )}/users/${Api.encodePath(username)}`,
                  {},
                );
              } catch (e) {
                AppToaster.show({
                  intent: Intent.DANGER,
                  message: `Could not create user '${username}' in the authorization system: ${getErrorMessage(
                    e,
                  )}`,
                });
                return;
              }
            }

            if (password) {
              try {
                await Api.instance.post(
                  `/proxy/coordinator/druid-ext/basic-security/authentication/db/${Api.encodePath(
                    authenticatorName,
                  )}/users/${Api.encodePath(username)}/credentials`,
                  { password },
                );
              } catch (e) {
                AppToaster.show({
                  intent: Intent.DANGER,
                  message: `Could not set password for user '${username}': ${getErrorMessage(e)}`,
                });
                return;
              }
            }

            AppToaster.show({
              intent: Intent.SUCCESS,
              message: `User '${username}' ${user ? 'updated' : 'created'} successfully`,
            });
            userQueryManager.rerunLastQuery();
          }}
          onClose={() => setOpenUserEditorOn(undefined)}
        />
      )}
      {openRoleEditorOn && (
        <RoleEditorDialog
          initRole={openRoleEditorOn.role}
          onSave={async (roleName, permissions) => {
            const role = openRoleEditorOn?.role;
            if (!role) {
              try {
                await Api.instance.post(
                  `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                    authorizerName,
                  )}/roles/${Api.encodePath(roleName)}`,
                  {},
                );
              } catch (e) {
                AppToaster.show({
                  intent: Intent.DANGER,
                  message: `Could not create role '${roleName}': ${getErrorMessage(e)}`,
                });
                return;
              }
            }

            const creatingRoleWithEmptyPermissions = !role && permissions.length === 0;
            if (!creatingRoleWithEmptyPermissions) {
              try {
                await Api.instance.post(
                  `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                    authorizerName,
                  )}/roles/${Api.encodePath(roleName)}/permissions`,
                  permissions,
                );
              } catch (e) {
                AppToaster.show({
                  intent: Intent.DANGER,
                  message: `Could not set permissions for role '${roleName}': ${getErrorMessage(
                    e,
                  )}`,
                });
                return;
              }
            }

            AppToaster.show({
              intent: Intent.SUCCESS,
              message: `Role '${roleName}' ${role ? 'updated' : 'created'} successfully`,
            });
            roleQueryManager.rerunLastQuery();
          }}
          onClose={() => setOpenRoleEditorOn(undefined)}
        />
      )}
      {assignRolesForUser && (
        <MultiSelectDialog
          title={`Assign roles to user '${assignRolesForUser.username}'`}
          initSelection={assignRolesForUser.roles || []}
          suggestions={(roleState.data || []).map(r => r.name)}
          onSave={async newRoleAssignment => {
            if (!assignRolesForUser) return;
            const { username, roles } = assignRolesForUser;

            // Role assignments that need to be added
            for (const role of newRoleAssignment) {
              if (!(roles || []).includes(role)) {
                try {
                  await Api.instance.post(
                    `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                      authorizerName,
                    )}/users/${Api.encodePath(username)}/roles/${Api.encodePath(role)}`,
                    {},
                  );
                } catch (e) {
                  AppToaster.show({
                    intent: Intent.DANGER,
                    message: `Could not assign role '${role}' to user '${username}': ${getErrorMessage(
                      e,
                    )}`,
                  });
                  return;
                }
              }
            }

            // Role assignments that need to be deleted
            for (const role of roles || []) {
              if (!newRoleAssignment.includes(role)) {
                try {
                  await Api.instance.delete(
                    `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                      authorizerName,
                    )}/users/${Api.encodePath(username)}/roles/${Api.encodePath(role)}`,
                  );
                } catch (e) {
                  AppToaster.show({
                    intent: Intent.DANGER,
                    message: `Could not un-assign role '${role}' from user '${username}': ${getErrorMessage(
                      e,
                    )}`,
                  });
                  return;
                }
              }
            }

            AppToaster.show({
              intent: Intent.SUCCESS,
              message: `User '${username}' role assignments updated`,
            });
            userQueryManager.rerunLastQuery();
            roleQueryManager.rerunLastQuery();
          }}
          onClose={() => setAssignRolesForUser(undefined)}
        />
      )}
      {assignUsersForRole && (
        <MultiSelectDialog
          title={`Assign users to role '${assignUsersForRole.name}'`}
          initSelection={assignUsersForRole.users || []}
          suggestions={(userState.data || []).map(u => u.username)}
          onSave={async newUserAssignment => {
            if (!assignUsersForRole) return;
            const { name, users } = assignUsersForRole;

            // User assignments that need to be added
            for (const user of newUserAssignment) {
              if (!(users || []).includes(user)) {
                try {
                  await Api.instance.post(
                    `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                      authorizerName,
                    )}/users/${Api.encodePath(user)}/roles/${Api.encodePath(name)}`,
                    {},
                  );
                } catch (e) {
                  AppToaster.show({
                    intent: Intent.DANGER,
                    message: `Could not assign user '${user}' to role '${name}': ${getErrorMessage(
                      e,
                    )}`,
                  });
                  return;
                }
              }
            }

            // User assignments that need to be deleted
            for (const user of users || []) {
              if (!newUserAssignment.includes(user)) {
                try {
                  await Api.instance.delete(
                    `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                      authorizerName,
                    )}/users/${Api.encodePath(user)}/roles/${Api.encodePath(name)}`,
                  );
                } catch (e) {
                  AppToaster.show({
                    intent: Intent.DANGER,
                    message: `Could not un-assign user '${user}' from role '${name}': ${getErrorMessage(
                      e,
                    )}`,
                  });
                  return;
                }
              }
            }

            AppToaster.show({
              intent: Intent.SUCCESS,
              message: `Role '${name}' user assignments updated`,
            });
            userQueryManager.rerunLastQuery();
            roleQueryManager.rerunLastQuery();
          }}
          onClose={() => setAssignUsersForRole(undefined)}
        />
      )}
      {userToDelete && (
        <AsyncActionDialog
          action={async () => {
            if (!userToDelete) return;
            try {
              await Api.instance.delete(
                `/proxy/coordinator/druid-ext/basic-security/authentication/db/${Api.encodePath(
                  authenticatorName,
                )}/users/${Api.encodePath(userToDelete.username)}`,
              );

              if (userToDelete.roles) {
                await Api.instance.delete(
                  `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                    authenticatorName,
                  )}/users/${Api.encodePath(userToDelete.username)}`,
                );
              }
            } catch (e) {
              throw new Error(getErrorMessage(e) || e.message);
            }
          }}
          confirmButtonText="Delete user"
          successText={`User '${userToDelete.username}' deleted successfully`}
          failText={`Could not delete user '${userToDelete.username}'`}
          intent={Intent.DANGER}
          onClose={() => {
            setUserToDelete(undefined);
          }}
          onSuccess={() => {
            userQueryManager.rerunLastQuery();
            roleQueryManager.rerunLastQuery();
          }}
        >
          <p>{`Are you sure you want to delete user '${
            userToDelete.username
          }' from the authentication ${
            userToDelete.roles ? 'and authorization systems' : 'system'
          }?`}</p>
        </AsyncActionDialog>
      )}
      {roleToDelete && (
        <AsyncActionDialog
          action={async () => {
            if (!roleToDelete) return;
            try {
              await Api.instance.delete(
                `/proxy/coordinator/druid-ext/basic-security/authorization/db/${Api.encodePath(
                  authorizerName,
                )}/roles/${Api.encodePath(roleToDelete.name)}`,
              );
            } catch (e) {
              throw new Error(getErrorMessage(e) || e.message);
            }
          }}
          confirmButtonText="Delete role"
          successText={`Role '${roleToDelete.name}' deleted successfully`}
          failText={`Could not delete role '${roleToDelete.name}'`}
          intent={Intent.DANGER}
          onClose={() => {
            setRoleToDelete(undefined);
          }}
          onSuccess={() => {
            userQueryManager.rerunLastQuery();
            roleQueryManager.rerunLastQuery();
          }}
        >
          <p>{`Are you sure you want to delete role '${roleToDelete.name}' from the authorization system?`}</p>
        </AsyncActionDialog>
      )}
    </div>
  );
};
