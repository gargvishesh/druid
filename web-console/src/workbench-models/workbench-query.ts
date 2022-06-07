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

import { guessDataSourceNameFromInputSource } from '../druid-models';
import { ColumnMetadata, deepDelete, getContextFromSqlQuery } from '../utils';
import { QueryContext } from '../utils/query-context';

import { DRUID_ENGINES, DruidEngine } from './druid-engine';
import { LastExecution } from './execution';
import {
  ExternalConfig,
  externalConfigToInitQuery,
  externalConfigToTableExpression,
} from './external-config';
import { WorkbenchQueryPart } from './workbench-query-part';

export interface InsertIntoConfig {
  readonly tableName: string;
  readonly segmentGranularity: string;
}

export interface TabEntry {
  id: string;
  tabName: string;
  query: WorkbenchQuery;
}

// -----------------------------

export interface WorkbenchQueryValue {
  queryParts: WorkbenchQueryPart[];
  queryContext: QueryContext;
  engine?: DruidEngine;
  unlimited?: boolean;
}

export class WorkbenchQuery {
  static PREVIEW_LIMIT = 10000;
  static TMP_PREFIX = '_tmp_';
  static INLINE_DATASOURCE_MARKER = '__you_have_been_visited_by_talaria';

  private static sqlTaskEnabled = false;

  static blank(): WorkbenchQuery {
    return new WorkbenchQuery({
      queryContext: {},
      queryParts: [WorkbenchQueryPart.blank()],
    });
  }

  static setSqlTaskEnabled(enabled: boolean): void {
    WorkbenchQuery.sqlTaskEnabled = enabled;
  }

  static getSqlTaskEnabled(): boolean {
    return WorkbenchQuery.sqlTaskEnabled;
  }

  static fromEffectiveQueryAndContext(queryString: string, context: QueryContext): WorkbenchQuery {
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

    let retQuery = WorkbenchQuery.blank()
      .changeQueryString(queryString)
      .changeQueryContext(cleanContext);

    if (noSqlOuterLimit && !retQuery.getIngestDatasource()) {
      retQuery = retQuery.changeUnlimited(true);
    }

    return retQuery;
  }

  static getInsertOrReplaceLine(sqlString: string): string | undefined {
    return /(?:^|\n)\s*(INSERT\s+INTO[^\n]+|REPLACE\s+INTO[^\n]+)(?:\n|SELECT)/im.exec(
      sqlString,
    )?.[1];
  }

  static getPartitionedByLine(sqlString: string): string | undefined {
    return /\n\s*(PARTITIONED\s+BY[^\n]+)(?:\n|$)/im.exec(sqlString)?.[1];
  }

  static getClusteredByLine(sqlString: string): string | undefined {
    return /\n\s*(CLUSTERED\s+BY[^\n]+)(?:\n|$)/im.exec(sqlString)?.[1];
  }

  static commentOutIngestParts(sqlString: string): string {
    return sqlString.replace(
      /((?:^|\n)\s*)(INSERT\s+INTO|REPLACE\s+INTO|OVERWRITE|PARTITIONED\s+BY|CLUSTERED\s+BY)/gi,
      (_, spaces, thing) => `${spaces}--${thing.substr(2)}`,
    );
  }

  static commentOutNumTasks(sqlString: string): string {
    return sqlString.replace(/--:(context\s+talariaNumTasks\s*:)/g, '--$1');
  }

  public readonly queryParts: WorkbenchQueryPart[];
  public readonly queryContext: QueryContext;
  public readonly engine?: DruidEngine;
  public readonly unlimited?: boolean;

  constructor(value: WorkbenchQueryValue) {
    let queryParts = value.queryParts;
    if (!Array.isArray(queryParts) || !queryParts.length) {
      queryParts = [WorkbenchQueryPart.blank()];
    }
    if (!(queryParts instanceof WorkbenchQueryPart)) {
      queryParts = queryParts.map(p => new WorkbenchQueryPart(p));
    }
    this.queryParts = queryParts;
    this.queryContext = value.queryContext;
    this.engine = DRUID_ENGINES.includes(value.engine as any) ? value.engine : undefined;
    if (value.unlimited) this.unlimited = true;
  }

  public valueOf(): WorkbenchQueryValue {
    return {
      queryParts: this.queryParts,
      queryContext: this.queryContext,
      engine: this.engine,
      unlimited: this.unlimited,
    };
  }

  public changeQueryParts(queryParts: WorkbenchQueryPart[]): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryParts });
  }

  public changeQueryContext(queryContext: QueryContext): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryContext });
  }

  public changeEngine(engine: DruidEngine | undefined): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), engine });
  }

  public changeUnlimited(unlimited: boolean): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), unlimited });
  }

  public isTaskEngineNeeded(): boolean {
    return this.queryParts.some(part => part.isTaskEngineNeeded());
  }

  public getEffectiveEngine(): DruidEngine {
    const { engine } = this;
    if (engine) return engine;
    if (this.getLastPart().isJsonLike()) return 'native';
    return WorkbenchQuery.getSqlTaskEnabled() && this.isTaskEngineNeeded() ? 'sql-task' : 'sql';
  }

  private getLastPart(): WorkbenchQueryPart {
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

  public getLastExecution(): LastExecution | undefined {
    return this.getLastPart().lastExecution;
  }

  public getParsedQuery(): SqlQuery | undefined {
    return this.getLastPart().parsedQuery;
  }

  public isEmptyQuery(): boolean {
    return this.getLastPart().isEmptyQuery();
  }

  public isValid(): boolean {
    const lastPart = this.getLastPart();
    if (lastPart.isJsonLike() && !lastPart.validJson()) {
      return false;
    }

    return true;
  }

  public getIngestDatasource() {
    return this.getLastPart().getIngestDatasource();
  }

  public isIngestQuery(): boolean {
    return Boolean(this.getIngestDatasource());
  }

  private changeLastQueryPart(lastQueryPart: WorkbenchQueryPart): WorkbenchQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.slice(0, queryParts.length - 1).concat(lastQueryPart));
  }

  public changeQueryName(queryName: string): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryName(queryName));
  }

  public changeQueryString(queryString: string): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryString(queryString));
  }

  public changeCollapsed(collapsed: boolean): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeCollapsed(collapsed));
  }

  public changeLastExecution(lastExecution: LastExecution | undefined): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeLastExecution(lastExecution));
  }

  public clear(): WorkbenchQuery {
    return new WorkbenchQuery({
      queryParts: [],
      queryContext: {},
    });
  }

  public toggleUnlimited(): WorkbenchQuery {
    const { unlimited } = this;
    return this.changeUnlimited(!unlimited);
  }

  public materializeQuery(): WorkbenchQuery {
    const { query } = this.getApiQuery();
    if (typeof query !== 'string') return this;
    const lastPart = this.getLastPart();
    return this.changeQueryParts([
      new WorkbenchQueryPart({
        id: lastPart.id,
        queryName: lastPart.queryName,
        queryString: query,
      }),
    ]);
  }

  public explodeQuery(): WorkbenchQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.flatMap(queryPart => queryPart.explodeQueryPart()));
  }

  public makePreview(): WorkbenchQuery {
    if (!this.isIngestQuery()) return this;

    let ret: WorkbenchQuery = this;

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

    // Remove everything pertaining to INSERT INTO / REPLACE INTO from the query string
    const newQueryString = parsedQuery
      ? parsedQuery
          .changeInsertClause(undefined)
          .changeReplaceClause(undefined)
          .changePartitionedByClause(undefined)
          .changeClusteredByClause(undefined)
          .toString()
      : WorkbenchQuery.commentOutIngestParts(this.getQueryString());

    // Remove talariaNumTasks from the query string if it exists
    return ret.changeQueryString(WorkbenchQuery.commentOutNumTasks(newQueryString));
  }

  public getApiQuery(): {
    engine: DruidEngine;
    query: Record<string, any>;
    sqlPrefixLines?: number;
    cancelQueryId?: string;
  } {
    const { queryParts, queryContext, unlimited } = this;
    if (!queryParts.length) throw new Error(`should not get here`);
    const engine = this.getEffectiveEngine();

    const lastQueryPart = this.getLastPart();
    if (engine === 'native') {
      const query = Hjson.parse(lastQueryPart.queryString);
      query.context = { ...(query.context || {}), queryContext };

      let cancelQueryId = query.context.queryId;
      if (!cancelQueryId) {
        // If the queryId (sqlQueryId) is not explicitly set on the context generate one so it is possible to cancel the query.
        query.context.queryId = cancelQueryId = uuidv4();
      }

      return {
        engine,
        query,
        cancelQueryId,
      };
    }

    const insertQuery = this.isIngestQuery();

    const prefixParts = queryParts
      .slice(0, queryParts.length - 1)
      .filter(part => !part.getIngestDatasource());

    let sqlQuery = lastQueryPart.getSqlString();
    let queryPrepend = '';
    let queryAppend = '';

    if (prefixParts.length) {
      const insertIntoLine = WorkbenchQuery.getInsertOrReplaceLine(sqlQuery);
      if (insertIntoLine) {
        const partitionedByLine = WorkbenchQuery.getPartitionedByLine(sqlQuery);
        const clusteredByLine = WorkbenchQuery.getClusteredByLine(sqlQuery);

        queryPrepend += insertIntoLine + '\n';
        sqlQuery = WorkbenchQuery.commentOutIngestParts(sqlQuery);

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
    const context: QueryContext = {
      sqlOuterLimit: unlimited || insertQuery ? undefined : 1001,
      ...queryContext,
      ...inlineContext,
    };

    let cancelQueryId: string | undefined;
    if (engine === 'sql') {
      cancelQueryId = context.sqlQueryId;
      if (!cancelQueryId) {
        // If the sqlQueryId is not explicitly set on the context generate one so it is possible to cancel the query.
        context.sqlQueryId = cancelQueryId = uuidv4();
      }
    }

    return {
      engine,
      query: {
        query: sqlQuery,
        context,
        resultFormat: 'array',
        header: true,
        typesHeader: true,
        sqlTypesHeader: true,
      },
      sqlPrefixLines: prefixLines,
      cancelQueryId,
    };
  }

  public getInlineMetadata(): ColumnMetadata[] {
    const { queryParts } = this;
    if (!queryParts.length) return [];
    return queryParts.slice(0, queryParts.length - 1).flatMap(p => p.getInlineMetadata());
  }

  public getPrefix(index: number): WorkbenchQuery {
    return this.changeQueryParts(this.queryParts.slice(0, index + 1));
  }

  public getPrefixQueries(): WorkbenchQuery[] {
    return this.queryParts.slice(0, this.queryParts.length - 1).map((_, i) => this.getPrefix(i));
  }

  public applyUpdate(newQuery: WorkbenchQuery, index: number): WorkbenchQuery {
    return newQuery.changeQueryParts(newQuery.queryParts.concat(this.queryParts.slice(index + 1)));
  }

  public duplicate(): WorkbenchQuery {
    return this.changeQueryParts(this.queryParts.map(part => part.duplicate()));
  }

  public duplicateLast(): WorkbenchQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(queryParts.concat(last.duplicate()));
  }

  public addBlank(): WorkbenchQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(
      queryParts.slice(0, queryParts.length - 1).concat(
        last
          .changeQueryName(last.queryName || 'q')
          .changeCollapsed(true)
          .changeLastExecution(undefined),
        WorkbenchQueryPart.blank(),
      ),
    );
  }

  public remove(index: number): WorkbenchQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.filter((_, i) => i !== index));
  }

  public insertExternalPanel(externalConfig: ExternalConfig, isArrays: boolean[]): WorkbenchQuery {
    const { queryParts } = this;

    const newQueryPart = WorkbenchQueryPart.fromQueryString(
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
