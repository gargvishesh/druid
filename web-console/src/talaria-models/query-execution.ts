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

import { QueryResult, SqlExpression, SqlQuery, SqlWithQuery } from 'druid-query-toolkit';

import { deepGet, oneOf } from '../utils';
import { QueryContext } from '../utils/query-context';

import { StageDefinition } from './talaria-stage';

// Hack around the concept that we might get back a SqlWithQuery and will need to unpack it
function parseSqlQuery(queryString: string): SqlQuery | undefined {
  const q = SqlExpression.maybeParse(queryString);
  if (!q) return;
  if (q instanceof SqlWithQuery) return q.flattenWith();
  if (q instanceof SqlQuery) return q;
  return;
}

export interface TalariaTaskError {
  error: {
    errorCode: string;
    errorMessage?: string;
    [key: string]: any;
  };
  host?: string;
  taskId?: string;
  stageNumber?: number;
  exceptionStackTrace?: string;
}

type ExecutionDestination =
  | {
      type: 'taskReport';
    }
  | { type: 'dataSource'; dataSource: string; exists?: boolean }
  | { type: 'external' }
  | { type: 'download' };

export type QueryExecutionStatus = 'RUNNING' | 'FAILED' | 'SUCCESS';

export interface QueryExecutionValue {
  id: string;
  sqlQuery?: string;
  queryContext?: QueryContext;
  status?: QueryExecutionStatus;
  startTime?: Date;
  duration?: number;
  stages?: StageDefinition[];
  destination?: ExecutionDestination;
  result?: QueryResult;
  error?: TalariaTaskError;
}

export class QueryExecution {
  static validStatus(
    status: string | undefined,
  ): status is 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS' {
    return oneOf(status, 'WAITING', 'PENDING', 'RUNNING', 'FAILED', 'SUCCESS');
  }

  static normalizeAsyncStatus(
    state: 'INITIALIZED' | 'RUNNING' | 'COMPLETE' | 'FAILED' | 'UNDETERMINED',
  ): QueryExecutionStatus {
    switch (state) {
      case 'COMPLETE':
        return 'SUCCESS';

      case 'INITIALIZED':
      case 'UNDETERMINED':
        return 'RUNNING';

      default:
        return state;
    }
  }

  // Treat WAITING as PENDING since they are all the same as far as the UI is concerned
  static normalizeTaskStatus(
    status: 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS',
  ): QueryExecutionStatus {
    switch (status) {
      case 'SUCCESS':
      case 'FAILED':
        return status;

      default:
        return 'RUNNING';
    }
  }

  static fromAsyncStatus(
    asyncResult: any,
    sqlQuery?: string,
    queryContext?: QueryContext,
  ): QueryExecution {
    const status = QueryExecution.normalizeAsyncStatus(asyncResult.state);
    return new QueryExecution({
      id: asyncResult.asyncResultId,
      status: asyncResult.error ? 'FAILED' : status,
      sqlQuery,
      queryContext,
      error: asyncResult.error
        ? { error: { errorCode: 'AsyncError', errorMessage: JSON.stringify(asyncResult.error) } }
        : status === 'FAILED'
        ? {
            error: {
              errorCode: 'UnknownError',
              errorMessage:
                'Execution failed, there is no detail information, and there is no error in the status response',
            },
          }
        : undefined,
      destination:
        asyncResult.engine === 'Broker'
          ? {
              type: 'taskReport',
            }
          : undefined,
    });
  }

  static fromAsyncDetail(report: any): QueryExecution {
    // Must have status set for a valid report
    const id = deepGet(report, 'talariaStatus.taskId') || deepGet(report, 'talariaTask.payload.id');

    let status = deepGet(report, 'talariaStatus.payload.status');
    if (
      !status &&
      report.error &&
      !String(report.error).startsWith(`Can't find chatHandler for handler`) // Ignore this error in particular
    ) {
      status = 'FAILED';
    }

    if (typeof id !== 'string' || !QueryExecution.validStatus(status)) {
      throw new Error('Invalid payload');
    }

    let error: TalariaTaskError | undefined;
    if (status === 'FAILED') {
      error =
        deepGet(report, 'talariaStatus.payload.errorReport') ||
        (typeof report.error === 'string'
          ? { error: { errorCode: 'UnknownError', errorMessage: report.error } }
          : undefined);
    }

    const stages = deepGet(report, 'talariaStages.payload.stages');
    const startTime = new Date(deepGet(report, 'talariaStatus.payload.startTime'));
    const durationMs = deepGet(report, 'talariaStatus.payload.durationMs');
    let res = new QueryExecution({
      id,
      status: QueryExecution.normalizeTaskStatus(status),
      startTime: isNaN(startTime.getTime()) ? undefined : startTime,
      duration: typeof durationMs === 'number' ? durationMs : undefined,
      stages,
      error,
      destination: deepGet(report, 'talariaTask.payload.spec.destination'),
    });

    if (deepGet(report, 'talariaTask.payload.sqlQuery')) {
      res = res.changeSqlQuery(
        deepGet(report, 'talariaTask.payload.sqlQuery'),
        deepGet(report, 'talariaTask.payload.sqlQueryContext'),
      );
    }

    return res;
  }

  static fromResult(result: QueryResult): QueryExecution {
    return new QueryExecution({
      id: result.sqlQueryId || result.queryId || 'direct_result',
      status: 'SUCCESS',
      result,
    });
  }

  public readonly id: string;
  public readonly sqlQuery?: string;
  public readonly queryContext?: QueryContext;
  public readonly status?: QueryExecutionStatus;
  public readonly startTime?: Date;
  public readonly duration?: number;
  public readonly stages?: StageDefinition[];
  public readonly destination?: ExecutionDestination;
  public readonly result?: QueryResult;
  public readonly error?: TalariaTaskError;

  constructor(value: QueryExecutionValue) {
    this.id = value.id;
    this.sqlQuery = value.sqlQuery;
    this.queryContext = value.queryContext;
    this.status = value.status;
    this.startTime = value.startTime;
    this.duration = value.duration;
    this.stages = value.stages;
    this.destination = value.destination;
    this.result = value.result;
    this.error = value.error;
  }

  valueOf(): QueryExecutionValue {
    return {
      id: this.id,
      sqlQuery: this.sqlQuery,
      queryContext: this.queryContext,
      status: this.status,
      startTime: this.startTime,
      duration: this.duration,
      stages: this.stages,
      destination: this.destination,
      result: this.result,
      error: this.error,
    };
  }

  public changeSqlQuery(sqlQuery: string, queryContext?: QueryContext): QueryExecution {
    const value = this.valueOf();

    value.sqlQuery = sqlQuery;
    value.queryContext = queryContext;
    const parsedQuery = parseSqlQuery(sqlQuery);
    if (value.result && (parsedQuery || queryContext)) {
      value.result = value.result.attachQuery({ context: queryContext }, parsedQuery);
    }

    return new QueryExecution(value);
  }

  public changeDestination(destination: ExecutionDestination): QueryExecution {
    return new QueryExecution({
      ...this.valueOf(),
      destination,
    });
  }

  public changeResult(result: QueryResult): QueryExecution {
    return new QueryExecution({
      ...this.valueOf(),
      result: result.attachQuery({}, this.sqlQuery ? parseSqlQuery(this.sqlQuery) : undefined),
    });
  }

  public updateWith(newSummary: QueryExecution): QueryExecution {
    let nextSummary = newSummary;
    if (this.sqlQuery && !nextSummary.sqlQuery) {
      nextSummary = nextSummary.changeSqlQuery(this.sqlQuery, this.queryContext);
    }
    if (this.destination && !nextSummary.destination) {
      nextSummary = nextSummary.changeDestination(this.destination);
    }

    return nextSummary;
  }

  public attachErrorFromStatus(status: any): QueryExecution {
    const errorMsg = deepGet(status, 'status.errorMsg');

    return new QueryExecution({
      ...this.valueOf(),
      error: {
        error: {
          errorCode: 'UnknownError',
          errorMessage: errorMsg,
        },
      },
    });
  }

  public markDestinationDatasourceExists(): QueryExecution {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return this;

    return new QueryExecution({
      ...this.valueOf(),
      destination: {
        ...destination,
        exists: true,
      },
    });
  }

  public isWaitingForQuery(): boolean {
    const { status } = this;
    return status !== 'SUCCESS' && status !== 'FAILED';
  }

  public isFullyComplete(): boolean {
    if (this.isWaitingForQuery()) return false;

    const { status, destination } = this;
    if (status === 'SUCCESS' && destination?.type === 'dataSource') {
      return Boolean(destination.exists);
    }

    return true;
  }

  public getInsertDatasource(): string | undefined {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return;
    return destination.dataSource;
  }

  public isSuccessfulInsert(): boolean {
    return Boolean(
      this.isFullyComplete() && this.getInsertDatasource() && this.status === 'SUCCESS',
    );
  }

  public getErrorMessage(): string | undefined {
    const { error } = this;
    if (!error) return;
    return (
      (error.error.errorCode ? `${error.error.errorCode}: ` : '') +
      (error.error.errorMessage || (error.exceptionStackTrace || '').split('\n')[0])
    );
  }

  public getEndTime(): Date | undefined {
    const { startTime, duration } = this;
    if (!startTime || !duration) return;
    return new Date(startTime.valueOf() + duration);
  }
}
