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

import { TalariaSummary } from '../../talaria-models';
import { DruidError, QueryState } from '../../utils';

export class TalariaTabCache {
  private static readonly cache: Record<
    string,
    QueryState<TalariaSummary, DruidError, TalariaSummary>
  > = {};

  static storeState(
    id: string,
    report: QueryState<TalariaSummary, DruidError, TalariaSummary>,
  ): void {
    TalariaTabCache.cache[id] = report;
  }

  static getState(id: string): QueryState<TalariaSummary, DruidError, TalariaSummary> | undefined {
    return TalariaTabCache.cache[id];
  }

  static deleteState(id: string): void {
    delete TalariaTabCache.cache[id];
  }

  static deleteStates(ids: string[]): void {
    for (const id of ids) {
      delete TalariaTabCache.cache[id];
    }
  }
}
