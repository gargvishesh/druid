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

import { AxiosResponse, CancelToken } from 'axios';
import { QueryResult, SqlLiteral } from 'druid-query-toolkit';

import { Api } from '../../singletons';
import { QueryExecution } from '../../talaria-models';
import {
  deepGet,
  downloadHref,
  DruidError,
  IntermediateQueryState,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { QueryContext } from '../../utils/query-context';

export interface SubmitAsyncQueryOptions {
  query: string | Record<string, any>;
  context: QueryContext;
  skipResults?: boolean;
  prefixLines?: number;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
  onSubmitted?: (id: string) => void;
}

export async function submitAsyncQuery(
  options: SubmitAsyncQueryOptions,
): Promise<QueryExecution | IntermediateQueryState<QueryExecution>> {
  const {
    query,
    context,
    skipResults,
    prefixLines,
    cancelToken,
    preserveOnTermination,
    onSubmitted,
  } = options;

  let sqlQuery: string;
  let jsonQuery: Record<string, any>;
  if (typeof query === 'string') {
    sqlQuery = query;
    jsonQuery = {
      query: sqlQuery,
      resultFormat: 'array',
      header: true,
      typesHeader: true,
      sqlTypesHeader: true,
      context: context,
    };
  } else {
    sqlQuery = query.query;
    jsonQuery = {
      ...query,
      context: {
        ...(query.context || {}),
        ...context,
      },
    };
  }

  let asyncResp: AxiosResponse;

  try {
    asyncResp = await Api.instance.post(`/druid/v2/sql/async`, jsonQuery, { cancelToken });
  } catch (e) {
    const druidError = deepGet(e, 'response.data.error');
    if (!druidError) throw e;
    throw new DruidError(druidError, prefixLines);
  }

  let execution = QueryExecution.fromAsyncStatus(asyncResp.data, sqlQuery, context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (skipResults) {
    execution = execution.changeDestination({ type: 'download' });
  } else {
    execution = await updateExecutionWithResultsIfNeeded(execution, cancelToken);
  }

  execution = await updateExecutionWithDatasourceExistsIfNeeded(execution, cancelToken);

  if (execution.isFullyComplete()) return execution;

  if (cancelToken) {
    deleteAsyncQueryOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export interface ReattachAsyncQueryOptions {
  id: string;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
}

export async function reattachAsyncQuery(
  option: ReattachAsyncQueryOptions,
): Promise<QueryExecution | IntermediateQueryState<QueryExecution>> {
  const { id, cancelToken, preserveOnTermination } = option;
  let execution = await getAsyncExecution(id, cancelToken);

  execution = await updateExecutionWithDatasourceExistsIfNeeded(execution, cancelToken);

  if (execution.isFullyComplete()) return execution;

  if (cancelToken) {
    deleteAsyncQueryOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export async function updateExecutionWithAsyncIfNeeded(
  execution: QueryExecution,
  cancelToken?: CancelToken,
): Promise<QueryExecution> {
  if (!execution.isWaitingForQuery()) return execution;

  return execution.updateWith(await getAsyncExecution(execution.id, cancelToken));
}

export async function getAsyncExecution(
  id: string,
  cancelToken?: CancelToken,
): Promise<QueryExecution> {
  let detailResp: AxiosResponse | undefined;
  try {
    detailResp = await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(id)}`, {
      cancelToken,
    });
  } catch {}

  let execution: QueryExecution;
  if (detailResp) {
    execution = QueryExecution.fromAsyncDetail(detailResp.data);
  } else {
    const statusResp = await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(id)}/status`, {
      cancelToken,
    });

    execution = QueryExecution.fromAsyncStatus(statusResp.data);
  }

  execution = await updateExecutionWithResultsIfNeeded(execution, cancelToken);

  return execution;
}

async function updateExecutionWithResultsIfNeeded(
  execution: QueryExecution,
  cancelToken?: CancelToken,
): Promise<QueryExecution> {
  if (
    execution.status !== 'SUCCESS' ||
    execution.result ||
    execution.destination?.type !== 'taskReport'
  ) {
    return execution;
  }

  return execution.changeResult(await getAsyncResult(execution.id, cancelToken));
}

async function updateExecutionWithDatasourceExistsIfNeeded(
  execution: QueryExecution,
  cancelToken?: CancelToken,
): Promise<QueryExecution> {
  if (
    !(execution.destination?.type === 'dataSource' && !execution.destination.exists) ||
    execution.status !== 'SUCCESS'
  ) {
    return execution;
  }

  const loadingSegments = await hasLoadingSegments(execution.destination.dataSource, cancelToken);
  if (loadingSegments) return execution;

  return execution.markDestinationDatasourceExists();
}

async function hasLoadingSegments(
  datasource: string,
  _cancelToken?: CancelToken,
): Promise<boolean> {
  const segmentCheck = await queryDruidSql({
    query: `SELECT segment_id FROM sys.segments WHERE datasource = ${SqlLiteral.create(
      datasource,
    )} AND is_published = 1 AND is_overshadowed = 0 AND is_available = 0 LIMIT 1`,
  });

  return Boolean(segmentCheck.length);
}

function deleteAsyncQueryOnCancel(
  id: string,
  cancelToken: CancelToken,
  preserveOnTermination = false,
): void {
  void cancelToken.promise
    .then(cancel => {
      if (preserveOnTermination && cancel.message === QueryManager.TERMINATION_MESSAGE) return;
      return Api.instance.delete(`/druid/v2/sql/async/${Api.encodePath(id)}`);
    })
    .catch(() => {});
}

export async function getAsyncResult(id: string, cancelToken?: CancelToken): Promise<QueryResult> {
  const resultsResp = await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(id)}/results`, {
    cancelToken,
  });

  return QueryResult.fromRawResult(
    resultsResp.data,
    false,
    true,
    true,
    true,
  ).inflateDatesFromSqlTypes();
}

export async function talariaBackgroundStatusCheck(
  execution: QueryExecution,
  _query: any,
  cancelToken: CancelToken,
): Promise<QueryExecution | IntermediateQueryState<QueryExecution>> {
  execution = await updateExecutionWithAsyncIfNeeded(execution, cancelToken);
  execution = await updateExecutionWithDatasourceExistsIfNeeded(execution, cancelToken);

  if (!execution.isFullyComplete()) return new IntermediateQueryState(execution);

  return execution;
}

export async function talariaBackgroundResultStatusCheck(
  execution: QueryExecution,
  query: any,
  cancelToken: CancelToken,
): Promise<QueryResult | IntermediateQueryState<QueryExecution>> {
  return extractQueryResults(await talariaBackgroundStatusCheck(execution, query, cancelToken));
}

export function extractQueryResults(
  execution: QueryExecution | IntermediateQueryState<QueryExecution>,
): QueryResult | IntermediateQueryState<QueryExecution> {
  if (execution instanceof IntermediateQueryState) return execution;

  if (execution.result) {
    return execution.result;
  } else {
    throw new Error(execution.getErrorMessage() || 'unexpected destination');
  }
}

export function downloadQueryResults(id: string, filename: string): void {
  downloadHref({
    href: `/druid/v2/sql/async/${Api.encodePath(id)}/results`,
    filename: filename,
  });
}
