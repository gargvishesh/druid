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

export type PermissionType =
  | 'DATASOURCE'
  | 'VIEW'
  | 'CONFIG'
  | 'STATE'
  | 'SYSTEM_TABLE'
  | 'QUERY_CONTEXT'
  | 'EXTERNAL';

export interface Permission {
  resource: {
    type: PermissionType;
    name: string;
  };
  action: 'READ' | 'WRITE';
}

export const PERMISSION_TYPES: PermissionType[] = [
  'DATASOURCE',
  'VIEW',
  'CONFIG',
  'STATE',
  'SYSTEM_TABLE',
  'QUERY_CONTEXT',
  'EXTERNAL',
];

export function formatPermission(permission: Permission): string {
  return `${permission.action} ${permission.resource.type}: ${permission.resource.name}`;
}

export interface UserEntry {
  username: string;
  credentials?: boolean;
  roles?: string[];
}

export interface RoleEntry {
  name: string;
  users?: string[];
  permissions?: Permission[];
}

export const ALL_PERMISSIONS: Permission[] = PERMISSION_TYPES.flatMap(type => {
  return [
    {
      resource: {
        name: '.*',
        type,
      },
      action: 'READ',
    },
    {
      resource: {
        name: '.*',
        type,
      },
      action: 'WRITE',
    },
  ];
});
