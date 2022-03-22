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

import { SqlQuery, SqlTableRef } from 'druid-query-toolkit';
import Hjson from 'hjson';
import { v4 as uuidv4 } from 'uuid';

import { guessDataSourceNameFromInputSource } from '../druid-models/ingestion-spec';
import { ColumnMetadata, deepDelete, getContextFromSqlQuery } from '../utils';
import { QueryContext } from '../utils/query-context';

import { TalariaQueryPart } from './talaria-query-part';
import {
  ExternalConfig,
  externalConfigToInitQuery,
  externalConfigToTableExpression,
} from './talaria-query-pattern';

export interface InsertIntoConfig {
  readonly tableName: string;
  readonly segmentGranularity: string;
}

export interface TabEntry {
  id: string;
  tabName: string;
  query: TalariaQuery;
}

export type DruidEngine = 'broker' | 'async' | 'talaria';

// -----------------------------

export interface TalariaQueryValue {
  queryParts: TalariaQueryPart[];
  queryContext: QueryContext;
  engine?: DruidEngine;
  unlimited?: boolean;
}

export class TalariaQuery {
  static PREVIEW_LIMIT = 10000;
  static TMP_PREFIX = '_tmp_';

  static blank(): TalariaQuery {
    return new TalariaQuery({
      queryContext: {},
      queryParts: [TalariaQueryPart.blank()],
    });
  }

  static fromEffectiveQueryAndContext(queryString: string, context: QueryContext): TalariaQuery {
    const inlineContext = getContextFromSqlQuery(queryString);

    const noSqlOuterLimit = typeof context['sqlOuterLimit'] === 'undefined';
    const cleanContext = { ...context };
    for (const k in inlineContext) {
      if (cleanContext[k] === inlineContext[k]) {
        delete cleanContext[k];
      }
    }
    delete cleanContext['sqlQueryId'];
    delete cleanContext['sqlOuterLimit'];
    delete cleanContext['talaria'];

    let retQuery = TalariaQuery.blank()
      .changeQueryString(queryString)
      .changeQueryContext(cleanContext);

    if (noSqlOuterLimit && !retQuery.getInsertDatasource()) {
      retQuery = retQuery.changeUnlimited(true);
    }

    return retQuery;
  }

  static getInsertIntoLine(sqlString: string): string | undefined {
    return /(?:^|\n)\s*(INSERT\s+INTO[^\n]+)(?:\n|SELECT)/im.exec(sqlString)?.[1];
  }

  static getPartitionedByLine(sqlString: string): string | undefined {
    return /\n\s*(PARTITIONED\s+BY[^\n]+)(?:\n|$)/im.exec(sqlString)?.[1];
  }

  static getClusteredByLine(sqlString: string): string | undefined {
    return /\n\s*(CLUSTERED\s+BY[^\n]+)(?:\n|$)/im.exec(sqlString)?.[1];
  }

  static commentOutInsertInto(sqlString: string): string {
    return sqlString.replace(
      /((?:^|\n)\s*)((?:INSERT\s+INTO)|(?:PARTITIONED\s+BY)|(?:CLUSTERED\s+BY))/gi,
      (_, spaces, thing) => `${spaces}--${thing.substr(2)}`,
    );
  }

  static commentOutTalariaNumTasks(sqlString: string): string {
    return sqlString.replace(/--:(context\s+talariaNumTasks\s*:)/g, '--$1');
  }

  public readonly queryParts: TalariaQueryPart[];
  public readonly queryContext: QueryContext;
  public readonly engine?: DruidEngine;
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
    this.engine = value.engine;
    if (value.unlimited) this.unlimited = true;
  }

  public valueOf(): TalariaQueryValue {
    return {
      queryParts: this.queryParts,
      queryContext: this.queryContext,
      engine: this.engine,
      unlimited: this.unlimited,
    };
  }

  public changeQueryParts(queryParts: TalariaQueryPart[]): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), queryParts });
  }

  public changeQueryContext(queryContext: QueryContext): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), queryContext });
  }

  public changeEngine(engine: DruidEngine | undefined): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), engine });
  }

  public changeUnlimited(unlimited: boolean): TalariaQuery {
    return new TalariaQuery({ ...this.valueOf(), unlimited });
  }

  public isTalariaEngineNeeded(): boolean {
    return this.queryParts.some(part => part.isTalariaEngineNeeded());
  }

  public getEffectiveEngine(): DruidEngine {
    const { engine, queryContext } = this;
    if (engine) return engine;
    if (queryContext.talaria) return 'talaria';
    return this.isTalariaEngineNeeded() ? 'talaria' : 'broker';
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

  public getLastQueryId(): string | undefined {
    return this.getLastPart().lastQueryId;
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

  public changeLastQueryId(lastQueryId: string | undefined): TalariaQuery {
    return this.changeLastQueryPart(this.getLastPart().changeLastQueryId(lastQueryId));
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

  public makePreview(): TalariaQuery {
    if (!this.isInsertQuery()) return this;

    let ret: TalariaQuery = this;

    // Limit all the helper queries
    const parsedQuery = this.getParsedQuery();
    if (parsedQuery) {
      const fromExpression = parsedQuery.getFirstFromExpression();
      if (fromExpression instanceof SqlTableRef) {
        const firstTable = fromExpression.getTable();
        ret = ret.changeQueryParts(
          this.queryParts.map(queryPart =>
            queryPart.queryName === firstTable ? queryPart.addPreviewLimit() : queryPart,
          ),
        );
      }
    }

    // Remove talariaNumTasks from the context if it exists
    if (typeof this.queryContext.talariaNumTasks !== 'undefined') {
      ret = ret.changeQueryContext(deepDelete(this.queryContext, 'talariaNumTasks'));
    }

    // Remove everything pertaining to INSERT INTO from the query string
    const newQueryString = parsedQuery
      ? parsedQuery
          .changeInsertClause(undefined)
          .changePartitionedByClause(undefined)
          .changeClusteredByClause(undefined)
          .toString()
      : TalariaQuery.commentOutInsertInto(this.getQueryString());

    // Remove talariaNumTasks from the query string if it exists
    return ret.changeQueryString(TalariaQuery.commentOutTalariaNumTasks(newQueryString));
  }

  public getEffectiveQueryAndContext(): {
    query: string | Record<string, any>;
    context: QueryContext;
    prefixLines?: number;
    isAsync: boolean;
    isSql: boolean;
    cancelQueryId?: string;
  } {
    const { queryParts, queryContext, unlimited } = this;
    if (!queryParts.length) throw new Error(`should not get here`);
    const engine = this.getEffectiveEngine();

    const lastQueryPart = this.getLastPart();
    if (lastQueryPart.isJsonLike()) {
      const query = Hjson.parse(lastQueryPart.queryString);
      const isSql = typeof query.query === 'string';

      let context: QueryContext = queryContext;
      if (engine === 'talaria') {
        context = { ...context, talaria: true };
      }

      const queryIdKey = isSql ? 'sqlQueryId' : 'queryId';
      // Look for the queryId in the JSON itself (if native) or in the context object.
      let cancelQueryId = (isSql ? undefined : query.context?.queryId) || context[queryIdKey];
      if (!cancelQueryId) {
        // If the queryId (sqlQueryId) is not explicitly set on the context generate one so it is possible to cancel the query.
        cancelQueryId = uuidv4();
        context = { ...context, [queryIdKey]: cancelQueryId };
      }

      return {
        query,
        context,
        isAsync: engine !== 'async',
        isSql,
        cancelQueryId,
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
        const partitionedByLine = TalariaQuery.getPartitionedByLine(sqlQuery);
        const clusteredByLine = TalariaQuery.getClusteredByLine(sqlQuery);

        queryPrepend += insertIntoLine + '\n';
        sqlQuery = TalariaQuery.commentOutInsertInto(sqlQuery);

        if (clusteredByLine) {
          queryAppend = '\n' + clusteredByLine + queryAppend;
        }
        if (partitionedByLine) {
          queryAppend = '\n' + partitionedByLine + queryAppend;
        }
      }

      queryPrepend += 'WITH\n' + prefixParts.map(p => p.toWithPart()).join(',\n') + '\n(\n';
      queryAppend = '\n)' + queryAppend;
    }

    let prefixLines = 0;
    if (queryPrepend) {
      prefixLines = queryPrepend.split('\n').length - 1;
      sqlQuery = queryPrepend + sqlQuery + queryAppend;
    }

    const inlineContext = getContextFromSqlQuery(sqlQuery);
    let context: QueryContext = {
      sqlOuterLimit: unlimited || insertQuery ? undefined : 1001,
      ...queryContext,
      ...inlineContext,
    };

    if (engine === 'talaria') {
      context = { ...context, talaria: true };
    }

    let cancelQueryId = context.sqlQueryId;
    if (!context.talaria && !cancelQueryId) {
      // If the queryId (sqlQueryId) is not explicitly set on the context generate one so it is possible to cancel the query.
      cancelQueryId = uuidv4();
      context = { ...context, sqlQueryId: cancelQueryId };
    }

    return {
      query: sqlQuery,
      context,
      prefixLines,
      isAsync: engine !== 'broker',
      isSql: true,
      cancelQueryId,
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

  public duplicate(): TalariaQuery {
    return this.changeQueryParts(this.queryParts.map(part => part.duplicate()));
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
          .changeLastQueryId(undefined),
        TalariaQueryPart.blank(),
      ),
    );
  }

  public remove(index: number): TalariaQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.filter((_, i) => i !== index));
  }

  public insertExternalPanel(externalConfig: ExternalConfig, isArrays: boolean[]): TalariaQuery {
    const { queryParts } = this;

    const newQueryPart = TalariaQueryPart.fromQueryString(
      'SELECT * FROM\n' + externalConfigToTableExpression(externalConfig),
      guessDataSourceNameFromInputSource(externalConfig.inputSource),
    ).changeCollapsed(true);

    let last = this.getLastPart();
    if (last.isEmptyQuery()) {
      last = last.changeQueryString(
        externalConfigToInitQuery(
          String(newQueryPart.queryName),
          externalConfig,
          isArrays,
        ).toString(),
      );
    }

    return this.changeQueryParts(
      queryParts.slice(0, queryParts.length - 1).concat(newQueryPart, last),
    );
  }
}
