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

import Hjson from 'hjson';

import { QueryContext } from '../query-context';

export function getContextFromSqlQuery(sql: string): QueryContext {
  const context: QueryContext = {};
  const lines = sql.split(/\r?\n/);
  for (const line of lines) {
    const m = /--:context\s*(.+)$/.exec(line);
    if (!m) continue;

    let lineContext: unknown;
    try {
      lineContext = Hjson.parse(m[1]);
    } catch {
      continue;
    }

    if (lineContext != null && typeof lineContext === 'object' && !Array.isArray(lineContext)) {
      Object.assign(context, lineContext);
    } else if (typeof lineContext === 'string') {
      if (lineContext.startsWith('!')) {
        context[lineContext.substr(1)] = false;
      } else {
        context[lineContext] = true;
      }
    }
  }
  return context;
}
