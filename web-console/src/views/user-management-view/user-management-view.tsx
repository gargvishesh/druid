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

import React from 'react';

import { Loader } from '../../components';
import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';

import { UsersAndRoles } from './users-and-roles';

import './user-management-view.scss';

function singleLoadedAuthThing(
  loadList: Record<string, boolean>,
  thing: 'authenticator' | 'authorizer',
): string {
  const keys = Object.keys(loadList);
  if (!keys.length) {
    throw new Error(`There isn't an ${thing} service configured.`);
  }
  if (keys.length > 1) {
    throw new Error(
      `There is more than one ${thing} service configured. Not sure which one to use for the UI please use the API instead.`,
    );
  }
  return keys[0];
}

interface AuthNames {
  authenticatorName: string;
  authorizerName: string;
}

export const UserManagementView = function UserManagementView() {
  const [loadStatusState] = useQueryManager<string, AuthNames>({
    initQuery: 'x',
    processQuery: async () => {
      const [authenticationLoadStatusResp, authorizationLoadStatusResp] = await Promise.all([
        Api.instance.get(`/proxy/coordinator/druid-ext/basic-security/authentication/loadStatus`),
        Api.instance.get(`/proxy/coordinator/druid-ext/basic-security/authorization/loadStatus`),
      ]);

      return {
        authenticatorName: singleLoadedAuthThing(
          authenticationLoadStatusResp.data,
          'authenticator',
        ),
        authorizerName: singleLoadedAuthThing(authorizationLoadStatusResp.data, 'authorizer'),
      };
    },
  });

  const loadStatusData = loadStatusState.data;
  const loadStatusError = loadStatusState.error;

  return (
    <div className="user-management-view app-view">
      {loadStatusState.loading && <Loader />}
      {loadStatusError &&
        ((loadStatusError as any).response?.status === 404 ? (
          <div>User management is not configured in this cluster.</div>
        ) : (
          <div>
            <div>Could not load the user management view due to an issue:</div>
            {loadStatusState.getErrorMessage()}
          </div>
        ))}
      {loadStatusData && (
        <UsersAndRoles
          authenticatorName={loadStatusData.authenticatorName}
          authorizerName={loadStatusData.authorizerName}
        />
      )}
    </div>
  );
};
