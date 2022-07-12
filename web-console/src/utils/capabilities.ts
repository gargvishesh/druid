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

import { Api } from '../singletons';

import { localStorageGetJson, LocalStorageKeys } from './local-storage-keys';
import { deepGet } from './object-change';

export type CapabilitiesMode = 'full' | 'no-sql' | 'no-proxy';

export type CapabilitiesModeExtended =
  | 'full'
  | 'no-sql'
  | 'no-proxy'
  | 'no-sql-no-proxy'
  | 'coordinator-overlord'
  | 'coordinator'
  | 'overlord';

export type QueryType = 'none' | 'nativeOnly' | 'nativeAndSql';

export interface CapabilitiesOptions {
  queryType: QueryType;
  msqe: boolean;
  coordinator: boolean;
  overlord: boolean;

  warnings?: string[];
}

export class Capabilities {
  static STATUS_TIMEOUT = 15000;
  static FULL: Capabilities;
  static NO_SQL: Capabilities;
  static COORDINATOR_OVERLORD: Capabilities;
  static COORDINATOR: Capabilities;
  static OVERLORD: Capabilities;
  static NO_PROXY: Capabilities;

  private readonly queryType: QueryType;
  private readonly msqe: boolean;
  private readonly coordinator: boolean;
  private readonly overlord: boolean;

  public readonly warnings: string[];

  static async detectQueryType(): Promise<QueryType | undefined> {
    // Check SQL endpoint
    try {
      await Api.instance.post(
        '/druid/v2/sql?capabilities',
        { query: 'SELECT 1337', context: { timeout: Capabilities.STATUS_TIMEOUT } },
        { timeout: Capabilities.STATUS_TIMEOUT },
      );
    } catch (e) {
      const { response } = e;
      if (response.status !== 405 && response.status !== 404) {
        return; // other failure
      }
      try {
        await Api.instance.get('/status?capabilities', { timeout: Capabilities.STATUS_TIMEOUT });
      } catch (e) {
        return; // total failure
      }
      // Status works but SQL 405s => the SQL endpoint is disabled

      try {
        await Api.instance.post(
          '/druid/v2?capabilities',
          {
            queryType: 'dataSourceMetadata',
            dataSource: '__web_console_probe__',
            context: { timeout: Capabilities.STATUS_TIMEOUT },
          },
          { timeout: Capabilities.STATUS_TIMEOUT },
        );
      } catch (e) {
        if (response.status !== 405 && response.status !== 404) {
          return; // other failure
        }

        return 'none';
      }

      return 'nativeOnly';
    }

    return 'nativeAndSql';
  }

  static async detectManagementProxy(): Promise<boolean> {
    try {
      await Api.instance.get(`/proxy/coordinator/status?capabilities`, {
        timeout: Capabilities.STATUS_TIMEOUT,
      });
    } catch (e) {
      return false;
    }

    return true;
  }

  static async detectNode(node: 'coordinator' | 'overlord'): Promise<boolean> {
    try {
      await Api.instance.get(
        `/druid/${node === 'overlord' ? 'indexer' : node}/v1/isLeader?capabilities`,
        {
          timeout: Capabilities.STATUS_TIMEOUT,
        },
      );
    } catch (e) {
      return false;
    }

    return true;
  }

  static async detectMsqe(): Promise<{ enabled: boolean; warning?: string }> {
    try {
      const resp = await Api.instance.get(`/druid/v2/sql/task/enabled?capabilities`);
      if (resp.data.enabled === true) {
        return { enabled: true };
      } else {
        return { enabled: false, warning: 'Unexpected response from /druid/v2/sql/task/enabled' };
      }
    } catch (e) {
      if (deepGet(e, 'response.status') === 404 && deepGet(e, 'response.data.enabled') === false) {
        return {
          enabled: false,
          warning:
            'The multi stage query engine extension is detected but is not properly configured. Please double check that you have set all the required server properties.',
        };
      } else {
        return { enabled: false };
      }
    }
  }

  static async detectCapabilities(): Promise<Capabilities | undefined> {
    const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);
    if (capabilitiesOverride) return new Capabilities(capabilitiesOverride);

    const queryType = await Capabilities.detectQueryType();
    if (typeof queryType === 'undefined') return;

    let coordinator: boolean;
    let overlord: boolean;
    if (queryType === 'none') {
      // must not be running on the router, figure out what node the console is on (or both?)
      coordinator = await Capabilities.detectNode('coordinator');
      overlord = await Capabilities.detectNode('overlord');
    } else {
      // must be running on the router, figure out if the management proxy is working
      coordinator = overlord = await Capabilities.detectManagementProxy();
    }

    const msqeStatus = await Capabilities.detectMsqe();

    return new Capabilities({
      queryType,
      msqe: msqeStatus.enabled,
      coordinator,
      overlord,
      warnings: msqeStatus.warning ? [msqeStatus.warning] : [],
    });
  }

  constructor(options: CapabilitiesOptions) {
    this.queryType = options.queryType;
    this.msqe = options.msqe;
    this.coordinator = options.coordinator;
    this.overlord = options.overlord;
    this.warnings = Array.isArray(options.warnings) ? options.warnings : [];
  }

  public hasWarnings(): boolean {
    return Boolean(this.warnings.length);
  }

  public getMode(): CapabilitiesMode {
    if (!this.hasSql()) return 'no-sql';
    if (!this.hasCoordinatorAccess()) return 'no-proxy';
    return 'full';
  }

  public getModeExtended(): CapabilitiesModeExtended | undefined {
    const { queryType, coordinator, overlord } = this;

    if (queryType === 'nativeAndSql') {
      if (coordinator && overlord) {
        return 'full';
      }
      if (!coordinator && !overlord) {
        return 'no-proxy';
      }
    } else if (queryType === 'nativeOnly') {
      if (coordinator && overlord) {
        return 'no-sql';
      }
      if (!coordinator && !overlord) {
        return 'no-sql-no-proxy';
      }
    } else {
      if (coordinator && overlord) {
        return 'coordinator-overlord';
      }
      if (coordinator) {
        return 'coordinator';
      }
      if (overlord) {
        return 'overlord';
      }
    }

    return;
  }

  public hasEverything(): boolean {
    return this.queryType === 'nativeAndSql' && this.coordinator && this.overlord;
  }

  public hasQuerying(): boolean {
    return this.queryType !== 'none';
  }

  public hasSql(): boolean {
    return this.queryType === 'nativeAndSql';
  }

  public hasMsqe(): boolean {
    return this.msqe;
  }

  public hasCoordinatorAccess(): boolean {
    return this.coordinator;
  }

  public hasSqlOrCoordinatorAccess(): boolean {
    return this.hasSql() || this.hasCoordinatorAccess();
  }

  public hasOverlordAccess(): boolean {
    return this.overlord;
  }

  public hasSqlOrOverlordAccess(): boolean {
    return this.hasSql() || this.hasOverlordAccess();
  }
}
Capabilities.FULL = new Capabilities({
  queryType: 'nativeAndSql',
  msqe: true,
  coordinator: true,
  overlord: true,
});
Capabilities.NO_SQL = new Capabilities({
  queryType: 'nativeOnly',
  msqe: false,
  coordinator: true,
  overlord: true,
});
Capabilities.COORDINATOR_OVERLORD = new Capabilities({
  queryType: 'none',
  msqe: false,
  coordinator: true,
  overlord: true,
});
Capabilities.COORDINATOR = new Capabilities({
  queryType: 'none',
  msqe: false,
  coordinator: true,
  overlord: false,
});
Capabilities.OVERLORD = new Capabilities({
  queryType: 'none',
  msqe: false,
  coordinator: false,
  overlord: true,
});
Capabilities.NO_PROXY = new Capabilities({
  queryType: 'nativeAndSql',
  msqe: true,
  coordinator: false,
  overlord: false,
});
