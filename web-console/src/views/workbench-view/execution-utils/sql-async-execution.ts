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
import { QueryResult } from 'druid-query-toolkit';

import { Api } from '../../../singletons';
import { Execution } from '../../../talaria-models';
import {
  deepGet,
  downloadHref,
  DruidError,
  IntermediateQueryState,
  QueryManager,
} from '../../../utils';
import { QueryContext } from '../../../utils/query-context';

export interface SubmitAsyncQueryOptions {
  query: string | Record<string, any>;
  context?: QueryContext;
  skipResults?: boolean;
  prefixLines?: number;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
  onSubmitted?: (id: string) => void;
}

export async function submitAsyncQuery(
  options: SubmitAsyncQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
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

    if (context) {
      jsonQuery = {
        ...query,
        context: {
          ...(query.context || {}),
          ...context,
        },
      };
    } else {
      jsonQuery = query;
    }
  }

  let asyncResp: AxiosResponse;

  try {
    asyncResp = await Api.instance.post(`/druid/v2/sql/async`, jsonQuery, { cancelToken });
  } catch (e) {
    const druidError = deepGet(e, 'response.data.error');
    if (!druidError) throw e;
    throw new DruidError(druidError, prefixLines);
  }

  let execution = Execution.fromAsyncStatus(asyncResp.data, sqlQuery, context);

  if (onSubmitted) {
    onSubmitted(execution.id);
  }

  if (skipResults) {
    execution = execution.changeDestination({ type: 'download' });
  } else {
    execution = await updateExecutionWithResultsIfNeeded(execution, cancelToken);
  }

  if (execution.isFullyComplete()) return execution;

  if (cancelToken) {
    cancelAsyncExecutionOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export interface ReattachAsyncQueryOptions {
  id: string;
  cancelToken?: CancelToken;
  preserveOnTermination?: boolean;
}

export async function reattachAsyncExecution(
  option: ReattachAsyncQueryOptions,
): Promise<Execution | IntermediateQueryState<Execution>> {
  const { id, cancelToken, preserveOnTermination } = option;
  const execution = await getAsyncExecution(id, cancelToken);
  if (execution.isFullyComplete()) return execution;

  if (cancelToken) {
    cancelAsyncExecutionOnCancel(execution.id, cancelToken, Boolean(preserveOnTermination));
  }

  return new IntermediateQueryState(execution);
}

export async function updateExecutionWithAsyncIfNeeded(
  execution: Execution,
  cancelToken?: CancelToken,
): Promise<Execution> {
  if (!execution.isWaitingForQuery()) return execution;

  return execution.updateWith(await getAsyncExecution(execution.id, cancelToken));
}

export async function getAsyncExecution(id: string, cancelToken?: CancelToken): Promise<Execution> {
  const encodedId = Api.encodePath(id);

  const statusResp = await Api.instance.get(`/druid/v2/sql/async/${encodedId}/status`, {
    cancelToken,
  });

  let execution = Execution.fromAsyncStatus(statusResp.data);

  execution = await updateExecutionWithResultsIfNeeded(execution, cancelToken);

  return execution;
}

async function updateExecutionWithResultsIfNeeded(
  execution: Execution,
  cancelToken?: CancelToken,
): Promise<Execution> {
  if (
    execution.status !== 'SUCCESS' ||
    execution.result ||
    (execution.destination && execution.destination.type !== 'taskReport')
  ) {
    return execution;
  }

  let asyncResult: QueryResult;
  if (execution.destination) {
    asyncResult = await getAsyncResult(execution.id, cancelToken);
  } else {
    // If destination is unknown, swallow the error
    try {
      asyncResult = await getAsyncResult(execution.id, cancelToken);
    } catch {
      return execution;
    }
  }

  return execution.changeResult(asyncResult);
}

function cancelAsyncExecutionOnCancel(
  id: string,
  cancelToken: CancelToken,
  preserveOnTermination = false,
): void {
  void cancelToken.promise
    .then(cancel => {
      if (preserveOnTermination && cancel.message === QueryManager.TERMINATION_MESSAGE) return;
      return cancelAsyncExecution(id);
    })
    .catch(() => {});
}

export function cancelAsyncExecution(id: string): Promise<void> {
  return Api.instance.delete(`/druid/v2/sql/async/${Api.encodePath(id)}`);
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

export function downloadAsyncQueryResults(id: string, filename: string): void {
  downloadHref({
    href: `/druid/v2/sql/async/${Api.encodePath(id)}/results`,
    filename: filename,
  });
}
