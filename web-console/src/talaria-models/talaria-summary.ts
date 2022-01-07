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

import { Column, QueryResult, SqlQuery } from 'druid-query-toolkit';

import { deepGet, oneOf } from '../utils';
import { QueryContext } from '../utils/query-context';

import { getStartTime, StageDefinition } from './talaria-stage';

interface TalariaResultsPayload {
  results: any[][];
  signature: { name: string; type: string }[];
  sqlTypeNames?: string[];
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
  | { type: 'external' }
  | { type: 'dataSource'; dataSource: string; exists?: boolean };

export type TalariaSummarySource = 'init' | 'reattach' | 'report' | 'status';
export type TalariaSummaryStatus = 'PENDING' | 'RUNNING' | 'FAILED' | 'SUCCESS';

export interface TalariaSummaryValue {
  taskId: string;
  summarySource: TalariaSummarySource;
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

  static resultsPayloadToQueryResult(resultsPayload: TalariaResultsPayload, taskId: string) {
    const { results, signature, sqlTypeNames } = resultsPayload;
    return new QueryResult({
      header: signature.map(
        (sig, i) =>
          new Column({ name: sig.name, nativeType: sig.type, sqlType: sqlTypeNames?.[i] }),
      ),
      rows: results,
    })
      .inflateDatesFromSqlTypes()
      .changeResultContext({ taskId });
    // .changeQueryDuration(...) // ToDo: add query duration one day
  }

  static init(taskId: string, sqlQuery?: string, queryContext?: QueryContext): TalariaSummary {
    return new TalariaSummary({
      taskId,
      summarySource: 'init',
      sqlQuery,
      queryContext,
    });
  }

  static reattach(taskId: string): TalariaSummary {
    return new TalariaSummary({
      taskId,
      summarySource: 'reattach',
    });
  }

  static fromReport(report: any): TalariaSummary {
    // Must have status set for a valid report
    const taskId = deepGet(report, 'talariaStatus.taskId');
    const status = deepGet(report, 'talariaStatus.payload.status');
    if (typeof taskId !== 'string' || !TalariaSummary.validStatus(status)) {
      throw new Error('invalid status');
    }

    let error: TalariaTaskError | undefined;
    if (status === 'FAILED') {
      error = deepGet(report, 'talariaStatus.payload.errorReport');
    }

    const stages = deepGet(report, 'talariaStages.payload.stages') || [];
    const results = deepGet(report, 'talariaResults.payload');
    return new TalariaSummary({
      taskId,
      summarySource: 'report',
      status: TalariaSummary.normalizeStatus(status),
      startTime: getStartTime(stages),
      stages,
      destination: results ? { type: 'taskReport' } : undefined,
      result: results ? TalariaSummary.resultsPayloadToQueryResult(results, taskId) : undefined,
      error,
    });
  }

  static fromStatus(statusPayload: any): TalariaSummary {
    const taskId = deepGet(statusPayload, 'task');
    const status = deepGet(statusPayload, 'status.status');
    if (typeof taskId !== 'string' || !TalariaSummary.validStatus(status)) {
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
      taskId,
      summarySource: 'status',
      status: status === 'WAITING' ? 'PENDING' : status, // Treat WAITING as PENDING since they are all the same for the UI
      startTime,
      error,
    });
  }

  static fromResult(result: QueryResult): TalariaSummary {
    return new TalariaSummary({
      taskId: result.sqlQueryId || result.queryId || 'direct_result',
      summarySource: 'init',
      status: 'SUCCESS',
      result,
    });
  }

  public readonly taskId: string;
  public readonly summarySource: TalariaSummarySource;
  public readonly sqlQuery?: string;
  public readonly queryContext?: QueryContext;
  public readonly status?: TalariaSummaryStatus;
  public readonly startTime?: Date;
  public readonly stages?: StageDefinition[];
  public readonly destination?: TalariaDestination;
  public readonly result?: QueryResult;
  public readonly error?: TalariaTaskError;

  constructor(value: TalariaSummaryValue) {
    this.taskId = value.taskId;
    this.summarySource = value.summarySource;
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
      taskId: this.taskId,
      summarySource: this.summarySource,
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

  public changeSqlQuery(sqlQuery: string, queryContext?: QueryContext): TalariaSummary {
    const value = this.valueOf();

    value.sqlQuery = sqlQuery;
    value.queryContext = queryContext;
    const parsedQuery = SqlQuery.maybeParse(sqlQuery);
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

  public updateWith(newSummary: TalariaSummary): TalariaSummary {
    if (this.stages && !newSummary.stages) return this;

    let nextSummary = newSummary;
    if (this.sqlQuery && !nextSummary.sqlQuery) {
      nextSummary = nextSummary.changeSqlQuery(this.sqlQuery, this.queryContext);
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
    return this.summarySource === 'reattach';
  }

  public isWaitingForTask(): boolean {
    const { status } = this;
    return status !== 'SUCCESS' && status !== 'FAILED';
  }

  public isFullyComplete(): boolean {
    if (this.isWaitingForTask()) return false;

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
