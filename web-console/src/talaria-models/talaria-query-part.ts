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

import { SqlExpression, SqlQuery, SqlRef, SqlTableRef, SqlWithQuery } from 'druid-query-toolkit';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';

import { ColumnMetadata, compact, filterMap } from '../utils';

import { generate8HexId } from './talaria-general';
import { fitExternalConfigPattern } from './talaria-query-pattern';

function sqlTypeFromDruid(druidType: string): string {
  druidType = druidType.toLowerCase();
  switch (druidType) {
    case 'string':
      return 'VARCHAR';

    case 'long':
      return 'BIGINT';

    case 'float':
    case 'double':
      return druidType.toUpperCase();

    default:
      return 'COMPLEX';
  }
}

// -----------------------------

export interface TalariaQueryPartValue {
  id: string;
  queryName?: string;
  queryString: string;
  collapsed?: boolean;
  lastQueryId?: string;
}

export class TalariaQueryPart {
  static blank() {
    return new TalariaQueryPart({
      id: generate8HexId(),
      queryString: '',
    });
  }

  static fromQuery(query: SqlQuery, queryName?: string, collapsed?: boolean) {
    return this.fromQueryString(query.changeParens([]).toString(), queryName, collapsed);
  }

  static fromQueryString(queryString: string, queryName?: string, collapsed?: boolean) {
    return new TalariaQueryPart({
      id: generate8HexId(),
      queryName,
      queryString,
      collapsed,
    });
  }

  static isTalariaEngineNeeded(queryString: string): boolean {
    return /EXTERN\s*\(/im.test(queryString) || /INSERT\s+INTO/im.test(queryString);
  }

  public readonly id: string;
  public readonly queryName?: string;
  public readonly queryString: string;
  public readonly collapsed: boolean;
  public readonly lastQueryId?: string;

  public readonly parsedQuery?: SqlQuery;

  constructor(value: TalariaQueryPartValue) {
    this.id = value.id;
    this.queryName = value.queryName;
    this.queryString = value.queryString;
    this.collapsed = Boolean(value.collapsed);
    this.lastQueryId = value.lastQueryId;

    try {
      this.parsedQuery = SqlQuery.parse(this.queryString);
    } catch {}
  }

  public valueOf(): TalariaQueryPartValue {
    return {
      id: this.id,
      queryName: this.queryName,
      queryString: this.queryString,
      collapsed: this.collapsed,
      lastQueryId: this.lastQueryId,
    };
  }

  public changeId(id: string): TalariaQueryPart {
    return new TalariaQueryPart({ ...this.valueOf(), id });
  }

  public changeQueryName(queryName: string): TalariaQueryPart {
    return new TalariaQueryPart({ ...this.valueOf(), queryName });
  }

  public changeQueryString(queryString: string): TalariaQueryPart {
    return new TalariaQueryPart({ ...this.valueOf(), queryString });
  }

  public changeCollapsed(collapsed: boolean): TalariaQueryPart {
    return new TalariaQueryPart({ ...this.valueOf(), collapsed });
  }

  public changeLastQueryId(lastQueryId: string | undefined): TalariaQueryPart {
    return new TalariaQueryPart({ ...this.valueOf(), lastQueryId });
  }

  public clear(): TalariaQueryPart {
    return new TalariaQueryPart({
      ...this.valueOf(),
      queryString: '',
    });
  }

  public isEmptyQuery(): boolean {
    return this.queryString.trim() === '';
  }

  public isJsonLike(): boolean {
    return this.queryString.trim().startsWith('{');
  }

  public validRune(): boolean {
    try {
      Hjson.parse(this.queryString);
      return true;
    } catch {
      return false;
    }
  }

  public prettyPrintJson(): TalariaQueryPart {
    let parsed: unknown;
    try {
      parsed = Hjson.parse(this.queryString);
    } catch {
      return this;
    }
    return this.changeQueryString(JSONBig.stringify(parsed, undefined, 2));
  }

  public getInsertDatasource(): string | undefined {
    const { queryString, parsedQuery } = this;
    if (parsedQuery) {
      return parsedQuery.getInsertIntoTable()?.getTable();
    }

    if (this.isJsonLike()) return;

    // When the parser fails try a regexp
    const m = /INSERT\s+INTO\s*([^\n]+)/im.exec(queryString);
    if (m) {
      try {
        const ex = SqlExpression.parse(m[1]);
        if (ex instanceof SqlRef) {
          return ex.getColumn();
        }
      } catch {}
    }

    return;
  }

  public getInlineMetadata(): ColumnMetadata[] {
    const { queryName, parsedQuery } = this;
    if (queryName && parsedQuery) {
      try {
        return fitExternalConfigPattern(parsedQuery).columns.map(({ name, type }) => ({
          COLUMN_NAME: name,
          DATA_TYPE: sqlTypeFromDruid(type),
          TABLE_NAME: queryName,
          TABLE_SCHEMA: 'druid',
        }));
      } catch {
        return filterMap(parsedQuery.getSelectExpressionsArray(), ex => {
          const outputName = ex.getOutputName();
          if (!outputName) return;
          return {
            COLUMN_NAME: outputName,
            DATA_TYPE: 'UNKNOWN',
            TABLE_NAME: queryName,
            TABLE_SCHEMA: 'druid',
          };
        });
      }
    }
    return [];
  }

  public isTalariaEngineNeeded(): boolean {
    return TalariaQueryPart.isTalariaEngineNeeded(this.queryString);
  }

  public explodeQueryPart(): TalariaQueryPart[] {
    let flatQuery: SqlQuery;
    try {
      // We need to do our own parsing here because this.parseQuery necessarily must be a SqlQuery
      // object, and we might have a SqlWithQuery here.
      flatQuery = (SqlExpression.parse(this.queryString) as SqlWithQuery).flattenWith();
    } catch {
      return [this];
    }

    const possibleNewParts = flatQuery.getWithParts().map(({ table, columns, query }) => {
      if (columns) return;
      return TalariaQueryPart.fromQuery(query, table.name, true);
    });

    const newParts = compact(possibleNewParts);
    if (newParts.length !== possibleNewParts.length) {
      return [this];
    }

    return newParts.concat(this.changeQueryString(flatQuery.changeWithParts(undefined).toString()));
  }

  public toWithPart(): string {
    const { queryName, queryString } = this;
    return `${SqlTableRef.create(queryName || 'q')} AS (\n${queryString}\n)`;
  }

  public duplicate(): TalariaQueryPart {
    return this.changeId(generate8HexId()).changeLastQueryId(undefined);
  }
}
