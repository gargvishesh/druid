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

import { getStartTime, StageDefinition } from './talaria-stage';

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

type TalariaDestination =
  | {
      type: 'taskReport';
    }
  | { type: 'dataSource'; dataSource: string; exists?: boolean }
  | { type: 'external' }
  | { type: 'download' };

export type TalariaSummarySource = 'init' | 'reattach' | 'detail' | 'status';
export type TalariaSummaryStatus = 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS';

export interface TalariaSummaryValue {
  id: string;
  source: TalariaSummarySource;
  sqlQuery?: string;
  queryContext?: QueryContext;
  status?: TalariaSummaryStatus;
  startTime?: Date;
  stages?: StageDefinition[];
  destination?: TalariaDestination;
  result?: QueryResult;
  error?: TalariaTaskError;
}

export class TalariaSummary {
  static validStatus(
    status: string | undefined,
  ): status is 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS' {
    return oneOf(status, 'WAITING', 'PENDING', 'RUNNING', 'FAILED', 'SUCCESS');
  }

  // Treat WAITING as PENDING since they are all the same as far as the UI is concerned
  static normalizeStatus(
    status: 'WAITING' | 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS',
  ): 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS' {
    return status === 'WAITING' ? 'PENDING' : status;
  }

  static normalizeAsyncStatus(
    state: 'INITIALIZED' | 'RUNNING' | 'COMPLETE' | 'FAILED' | 'UNDETERMINED',
  ): 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS' {
    switch (state) {
      case 'COMPLETE':
        return 'SUCCESS';
      case 'INITIALIZED':
        return 'PENDING';
      case 'UNDETERMINED':
        return 'PENDING';
      default:
        return state;
    }
  }

  static init(id: string, sqlQuery?: string, queryContext?: QueryContext): TalariaSummary {
    return new TalariaSummary({
      id,
      source: 'init',
      sqlQuery,
      queryContext,
    });
  }

  static fromAsyncStatus(
    asyncResult: any,
    sqlQuery?: string,
    queryContext?: QueryContext,
  ): TalariaSummary {
    return new TalariaSummary({
      id: asyncResult.asyncResultId,
      source: 'status',
      status: TalariaSummary.normalizeAsyncStatus(asyncResult.state),
      sqlQuery,
      queryContext,
      error: asyncResult.error
        ? { error: { errorCode: 'AsyncError', errorMessage: JSON.stringify(asyncResult.error) } }
        : undefined,
      destination:
        asyncResult.engine === 'Broker'
          ? {
              type: 'taskReport',
            }
          : undefined,
    });
  }

  static reattach(id: string): TalariaSummary {
    return new TalariaSummary({
      id,
      source: 'reattach',
    });
  }

  static fromDetail(report: any): TalariaSummary {
    // Must have status set for a valid report
    const id = deepGet(report, 'talariaStatus.taskId');
    const status = deepGet(report, 'talariaStatus.payload.status');
    if (typeof id !== 'string' || !TalariaSummary.validStatus(status)) {
      throw new Error('invalid status');
    }

    let error: TalariaTaskError | undefined;
    if (status === 'FAILED') {
      error = deepGet(report, 'talariaStatus.payload.errorReport');
    }

    const stages = deepGet(report, 'talariaStages.payload.stages') || [];
    return new TalariaSummary({
      id,
      source: 'detail',
      status: TalariaSummary.normalizeStatus(status),
      startTime: getStartTime(stages),
      stages,
      error,
    });
  }

  static fromStatus(statusPayload: any): TalariaSummary {
    const id = deepGet(statusPayload, 'task');
    const status = deepGet(statusPayload, 'status.status');
    if (typeof id !== 'string' || !TalariaSummary.validStatus(status)) {
      throw new Error(`invalid task status from status endpoint: ${status}`);
    }

    let error: TalariaTaskError | undefined;
    if (status === 'FAILED') {
      const errorMsg = deepGet(statusPayload, 'status.errorMsg');
      if (errorMsg) {
        error = {
          error: { errorCode: 'UnknownError', errorMessage: errorMsg },
        };
      }
    }

    let startTime: Date | undefined = new Date(deepGet(statusPayload, 'createdTime'));
    if (isNaN(startTime.valueOf())) startTime = undefined;

    return new TalariaSummary({
      id: id,
      source: 'status',
      status: status === 'WAITING' ? 'PENDING' : status, // Treat WAITING as PENDING since they are all the same for the UI
      startTime,
      error,
    });
  }

  static fromResult(result: QueryResult): TalariaSummary {
    return new TalariaSummary({
      id: result.sqlQueryId || result.queryId || 'direct_result',
      source: 'init',
      status: 'SUCCESS',
      result,
    });
  }

  public readonly id: string;
  public readonly source: TalariaSummarySource;
  public readonly sqlQuery?: string;
  public readonly queryContext?: QueryContext;
  public readonly status?: TalariaSummaryStatus;
  public readonly startTime?: Date;
  public readonly stages?: StageDefinition[];
  public readonly destination?: TalariaDestination;
  public readonly result?: QueryResult;
  public readonly error?: TalariaTaskError;

  constructor(value: TalariaSummaryValue) {
    this.id = value.id;
    this.source = value.source;
    this.sqlQuery = value.sqlQuery;
    this.queryContext = value.queryContext;
    this.status = value.status;
    this.startTime = value.startTime;
    this.stages = value.stages;
    this.destination = value.destination;
    this.result = value.result;
    this.error = value.error;
  }

  valueOf(): TalariaSummaryValue {
    return {
      id: this.id,
      source: this.source,
      sqlQuery: this.sqlQuery,
      queryContext: this.queryContext,
      status: this.status,
      startTime: this.startTime,
      stages: this.stages,
      destination: this.destination,
      result: this.result,
      error: this.error,
    };
  }

  public changeSummarySource(source: TalariaSummarySource): TalariaSummary {
    return new TalariaSummary({
      ...this.valueOf(),
      source,
    });
  }

  public changeSqlQuery(sqlQuery: string, queryContext?: QueryContext): TalariaSummary {
    const value = this.valueOf();

    value.sqlQuery = sqlQuery;
    value.queryContext = queryContext;
    const parsedQuery = parseSqlQuery(sqlQuery);
    if (value.result && (parsedQuery || queryContext)) {
      value.result = value.result.attachQuery({ context: queryContext }, parsedQuery);
    }

    return new TalariaSummary(value);
  }

  public changeDestination(destination: TalariaDestination): TalariaSummary {
    return new TalariaSummary({
      ...this.valueOf(),
      destination,
    });
  }

  public changeResult(result: QueryResult): TalariaSummary {
    return new TalariaSummary({
      ...this.valueOf(),
      result: result.attachQuery({}, this.sqlQuery ? parseSqlQuery(this.sqlQuery) : undefined),
    });
  }

  public updateWith(newSummary: TalariaSummary): TalariaSummary {
    if (this.stages && !newSummary.stages) return this;

    let nextSummary = newSummary;
    if (this.sqlQuery && !nextSummary.sqlQuery) {
      nextSummary = nextSummary.changeSqlQuery(this.sqlQuery, this.queryContext);
    }
    if (this.destination && !nextSummary.destination) {
      nextSummary = nextSummary.changeDestination(this.destination);
    }

    return nextSummary;
  }

  public attachErrorFromStatus(status: any): TalariaSummary {
    const errorMsg = deepGet(status, 'status.errorMsg');

    return new TalariaSummary({
      ...this.valueOf(),
      error: {
        error: {
          errorCode: 'UnknownError',
          errorMessage: errorMsg,
        },
      },
    });
  }

  public needsInfoFromPayload(): boolean {
    return Boolean(!this.destination || !this.sqlQuery);
  }

  public updateFromPayload(payload: any): TalariaSummary {
    return this.changeDestination(deepGet(payload, 'payload.spec.destination')).changeSqlQuery(
      deepGet(payload, 'payload.sqlQuery'),
      deepGet(payload, 'payload.sqlQueryContext'),
    );
  }

  public markDestinationDatasourceExists(): TalariaSummary {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return this;

    return new TalariaSummary({
      ...this.valueOf(),
      destination: {
        ...destination,
        exists: true,
      },
    });
  }

  public isReattach(): boolean {
    return this.source === 'reattach';
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

  public getDuration(): number {
    return 0;
  }

  public getInsertDataSource(): string | undefined {
    const { destination } = this;
    if (destination?.type !== 'dataSource') return;
    return destination.dataSource;
  }

  public isSuccessfulInsert(): boolean {
    return Boolean(
      this.isFullyComplete() && this.getInsertDataSource() && this.status === 'SUCCESS',
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
}
