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

import { SqlQuery } from 'druid-query-toolkit';
import Hjson from 'hjson';

import { guessDataSourceNameFromInputSource } from '../druid-models/ingestion-spec';
import { ColumnMetadata, getContextFromSqlQuery } from '../utils';
import { QueryContext } from '../utils/query-context';

import { LastQueryInfo, TalariaQueryPart } from './talaria-query-part';
import {
  ExternalConfig,
  externalConfigToInitQuery,
  externalConfigToTableExpression,
} from './talaria-query-pattern';

export interface InsertIntoConfig {
  readonly tableName: string;
  readonly segmentGranularity: string;
}

// -----------------------------

export interface TalariaQueryValue {
  queryParts: TalariaQueryPart[];
  queryContext: QueryContext;
  unlimited?: boolean;
}

export class TalariaQuery {
  static PREVIEW_LIMIT = 10000;

  static blank(): TalariaQuery {
    return new TalariaQuery({
      queryContext: {},
      queryParts: [TalariaQueryPart.blank()],
    });
  }

  static getInsertIntoLine(sqlString: string): string | undefined {
    return /(?:^|\n)\s*(INSERT\s+INTO[^\n]+)(?:\n|SELECT)/im.exec(sqlString)?.[1];
  }

  static commentOutInsertInto(sqlString: string): string {
    return sqlString.replace(
      /((?:^|\n)\s*)(INSERT\s+INTO)/i,
      (_, spaces, insertInto) => `${spaces}--${insertInto.substr(2)}`,
    );
  }

  public readonly queryParts: TalariaQueryPart[];
  public readonly queryContext: QueryContext;
  public readonly unlimited?: boolean;

  constructor(value: TalariaQueryValue) {
    let queryParts = value.queryParts;
    if (!Array.isArray(queryParts) || !queryParts.length) {
      queryParts = [TalariaQueryPart.blank()];
    }
    if (!(queryParts instanceof TalariaQueryPart)) {
      queryParts = queryParts.map(p => new TalariaQueryPart(p));
    }
    this.queryParts = queryParts;
    this.queryContext = value.queryContext;
    if (value.unlimited) this.unlimited = true;
  }

  public valueOf(): TalariaQueryValue {
    return {
      queryParts: this.queryParts,
      queryContext: this.queryContext,
      unlimited: this.unlimited,
    };
  }

  public changeQueryParts(queryParts: TalariaQueryPart[]): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), queryParts });
  }

  public changeQueryContext(queryContext: QueryContext): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), queryContext });
  }

  public changeUnlimited(unlimited: boolean): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), unlimited });
  }

  private getLastPart(): TalariaQueryPart {
    const { queryParts } = this;
    return queryParts[queryParts.length - 1];
  }

  public getId(): string {
    return this.getLastPart().id;
  }

  public getIds(): string[] {
    return this.queryParts.map(queryPart => queryPart.id);
  }

  public getQueryName(): string {
    return this.getLastPart().queryName || '';
  }

  public getQueryString(): string {
    return this.getLastPart().queryString;
  }

  public getCollapsed(): boolean {
    return this.getLastPart().collapsed;
  }

  public getLastQueryInfo(): LastQueryInfo | undefined {
    return this.getLastPart().lastQueryInfo;
  }

  public getParsedQuery(): SqlQuery | undefined {
    return this.getLastPart().parsedQuery;
  }

  public isEmptyQuery(): boolean {
    return this.getLastPart().isEmptyQuery();
  }

  public isJsonLike(): boolean {
    return this.getLastPart().isJsonLike();
  }

  public validRune(): boolean {
    return this.getLastPart().validRune();
  }

  public getInsertDatasource() {
    return this.getLastPart().getInsertDatasource();
  }

  public isInsertQuery(): boolean {
    return Boolean(this.getInsertDatasource());
  }

  private changeLastQueryPart(lastQueryPart: TalariaQueryPart): TalariaQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.slice(0, queryParts.length - 1).concat(lastQueryPart));
  }

  public changeQueryName(queryName: string): TalariaQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryName(queryName));
  }

  public changeQueryString(queryString: string): TalariaQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryString(queryString));
  }

  public changeCollapsed(collapsed: boolean): TalariaQuery {
    return this.changeLastQueryPart(this.getLastPart().changeCollapsed(collapsed));
  }

  public changeLastQueryInfo(lastQueryInfo: LastQueryInfo | undefined): TalariaQuery {
    return this.changeLastQueryPart(this.getLastPart().changeLastQueryInfo(lastQueryInfo));
  }

  public clear(): TalariaQuery {
    return new TalariaQuery({
      queryParts: [],
      queryContext: {},
    });
  }

  public toggleUnlimited(): TalariaQuery {
    const { unlimited } = this;
    return this.changeUnlimited(!unlimited);
  }

  public materializeQuery(): TalariaQuery {
    const { query } = this.getEffectiveQueryAndContext();
    if (typeof query !== 'string') return this;
    const lastPart = this.getLastPart();
    return this.changeQueryParts([
      new TalariaQueryPart({
        id: lastPart.id,
        queryName: lastPart.queryName,
        queryString: query,
      }),
    ]);
  }

  public explodeQuery(): TalariaQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.flatMap(queryPart => queryPart.explodeQueryPart()));
  }

  public removeInsert(): TalariaQuery {
    if (!this.isInsertQuery()) return this;

    const parsedQuery = this.getParsedQuery();
    return this.changeQueryString(
      parsedQuery
        ? parsedQuery.changeInsertClause(undefined).toString()
        : TalariaQuery.commentOutInsertInto(this.getQueryString()),
    );
  }

  public getEffectiveQueryAndContext(): {
    query: string | Record<string, any>;
    context: QueryContext;
    prefixLines?: number;
  } {
    const { queryParts, queryContext, unlimited } = this;
    if (!queryParts.length) throw new Error(`should not get here`);

    const lastQueryPart = this.getLastPart();
    if (lastQueryPart.isJsonLike()) {
      return {
        query: Hjson.parse(lastQueryPart.queryString),
        context: {
          ...queryContext,
          talaria: true,
        },
      };
    }

    const insertQuery = this.isInsertQuery();

    const prefixParts = queryParts
      .slice(0, queryParts.length - 1)
      .filter(part => !part.getInsertDatasource());

    let sqlQuery = lastQueryPart.queryString;
    let queryPrepend = '';
    let queryAppend = '';

    if (prefixParts.length) {
      const insertIntoLine = TalariaQuery.getInsertIntoLine(sqlQuery);
      if (insertIntoLine) {
        queryPrepend += insertIntoLine + '\n';
        sqlQuery = TalariaQuery.commentOutInsertInto(sqlQuery);
      }

      queryPrepend += 'WITH\n' + prefixParts.map(p => p.toWithPart()).join(',\n') + '\n(';
      queryAppend += '\n)';
    }

    let prefixLines = 0;
    if (queryPrepend) {
      prefixLines = queryPrepend.split('\n').length - 1;
      sqlQuery = queryPrepend + sqlQuery + queryAppend;
    }

    const inlineContext = getContextFromSqlQuery(sqlQuery);
    if (!insertQuery && inlineContext.talariaSegmentGranularity) {
      delete inlineContext.talariaSegmentGranularity;
    }

    return {
      query: sqlQuery,
      context: {
        sqlOuterLimit: unlimited || insertQuery ? undefined : 1001,
        ...queryContext,
        ...inlineContext,
        talaria: true,
      },
      prefixLines,
    };
  }

  public getInlineMetadata(): ColumnMetadata[] {
    const { queryParts } = this;
    if (!queryParts.length) return [];
    return queryParts.slice(0, queryParts.length - 1).flatMap(p => p.getInlineMetadata());
  }

  public getPrefix(index: number): TalariaQuery {
    return this.changeQueryParts(this.queryParts.slice(0, index + 1));
  }

  public getPrefixQueries(): TalariaQuery[] {
    return this.queryParts.slice(0, this.queryParts.length - 1).map((_, i) => this.getPrefix(i));
  }

  public applyUpdate(newQuery: TalariaQuery, index: number): TalariaQuery {
    return newQuery.changeQueryParts(newQuery.queryParts.concat(this.queryParts.slice(index + 1)));
  }

  public duplicateLast(): TalariaQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(queryParts.concat(last.duplicate()));
  }

  public addBlank(): TalariaQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(
      queryParts.slice(0, queryParts.length - 1).concat(
        last
          .changeQueryName(last.queryName || 'q')
          .changeCollapsed(true)
          .changeLastQueryInfo(undefined),
        TalariaQueryPart.blank(),
      ),
    );
  }

  public remove(index: number): TalariaQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.filter((_, i) => i !== index));
  }

  public insertExternalPanel(externalConfig: ExternalConfig): TalariaQuery {
    const { queryParts } = this;

    const newQueryPart = TalariaQueryPart.fromQueryString(
      'SELECT * FROM\n' + externalConfigToTableExpression(externalConfig),
      guessDataSourceNameFromInputSource(externalConfig.inputSource),
    ).changeCollapsed(true);

    let last = this.getLastPart();
    if (last.isEmptyQuery()) {
      last = last.changeQueryString(
        externalConfigToInitQuery(String(newQueryPart.queryName), externalConfig).toString(),
      );
    }

    return this.changeQueryParts(
      queryParts.slice(0, queryParts.length - 1).concat(newQueryPart, last),
    );
  }
}
