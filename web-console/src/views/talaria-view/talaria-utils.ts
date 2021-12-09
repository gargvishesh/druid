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

import { CancelToken } from 'axios';
import { QueryResult, SqlLiteral } from 'druid-query-toolkit';

import { Api } from '../../singletons';
import { TalariaQuery, TalariaSummary } from '../../talaria-models';
import { IntermediateQueryState, queryDruidSql, QueryManager, wait } from '../../utils';

export async function getTalariaSummaryFromReport(
  taskId: string,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  const reportsResp = await Api.instance.get(
    `/druid/indexer/v1/task/${Api.encodePath(taskId)}/reports`,
    { cancelToken },
  );

  let summary = TalariaSummary.fromReport(reportsResp.data);

  if (summary.status === 'FAILED' && !summary.error) {
    const statusResp = await Api.instance.get(
      `/druid/indexer/v1/task/${Api.encodePath(taskId)}/status`,
      {
        cancelToken,
      },
    );

    summary = summary.attachErrorFromStatus(statusResp.data);
  }

  return summary;
}

export async function getTalariaSummaryFromStatus(
  taskId: string,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  const statusResp = await Api.instance.get(
    `/druid/indexer/v1/task/${Api.encodePath(taskId)}/status`,
    {
      cancelToken,
    },
  );

  return TalariaSummary.fromStatus(statusResp.data);
}

export async function getTalariaSummary(
  taskId: string,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  try {
    return await getTalariaSummaryFromReport(taskId, cancelToken);
  } catch {
    // Retry once after delay
    await wait(500);
    try {
      return await getTalariaSummaryFromReport(taskId, cancelToken);
    } catch {}

    // If there is an issue with reports fallback to status endpoint
  }

  return await getTalariaSummaryFromStatus(taskId, cancelToken);
}

export async function updateSummaryWithReportIfNeeded(
  currentSummary: TalariaSummary,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (!currentSummary.isWaitingForTask()) return currentSummary;
  const newSummary = await getTalariaSummary(currentSummary.taskId, cancelToken);
  return currentSummary.updateWith(newSummary);
}

async function updateSummaryWithPayloadIfNeeded(
  currentSummary: TalariaSummary,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (!currentSummary.needsInfoFromPayload()) return currentSummary;

  const payloadResp = await Api.instance.get(
    `/druid/indexer/v1/task/${Api.encodePath(currentSummary.taskId)}`,
    { cancelToken },
  );

  return currentSummary.updateFromPayload(payloadResp.data);
}

async function updateSummaryWithDatasourceExistsIfNeeded(
  currentSummary: TalariaSummary,
  _cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (!(currentSummary.destination?.type === 'dataSource' && !currentSummary.destination.exists)) {
    return currentSummary;
  }

  // get the segments
  const tableCheck = await queryDruidSql({
    query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ${SqlLiteral.create(
      currentSummary.destination.dataSource,
    )}`,
  });
  if (tableCheck.length) {
    // Set exists
    return currentSummary.markDestinationDatasourceExists();
  } else {
    return currentSummary;
  }
}

export function getTaskIdFromQueryResults(queryResult: QueryResult): string | undefined {
  if (queryResult.getHeaderNames()[0] !== 'TASK' || queryResult.rows.length !== 1) return;
  return queryResult.rows[0][0];
}

export function killTaskOnCancel(
  taskId: string,
  cancelToken: CancelToken,
  preserveOnTermination = false,
): void {
  void cancelToken.promise
    .then(cancel => {
      if (preserveOnTermination && cancel.message === QueryManager.TERMINATION_MESSAGE) return;
      return Api.instance.post(`/druid/indexer/v1/task/${Api.encodePath(taskId)}/shutdown`, {});
    })
    .catch(() => {});
}

export async function talariaBackgroundStatusCheck(
  currentSummary: TalariaSummary,
  _query: any,
  cancelToken: CancelToken,
) {
  let updatedSummary = await updateSummaryWithReportIfNeeded(currentSummary, cancelToken);

  if (!updatedSummary.isWaitingForTask()) {
    updatedSummary = await updateSummaryWithPayloadIfNeeded(updatedSummary, cancelToken);
    updatedSummary = await updateSummaryWithDatasourceExistsIfNeeded(updatedSummary, cancelToken);
  }

  if (updatedSummary.isFullyComplete()) {
    return updatedSummary;
  } else {
    return new IntermediateQueryState(updatedSummary);
  }
}

export async function talariaBackgroundResultStatusCheck(
  currentSummary: TalariaSummary,
  _query: any,
  cancelToken: CancelToken,
) {
  const updatedSummary = await updateSummaryWithReportIfNeeded(currentSummary, cancelToken);

  if (updatedSummary.isWaitingForTask()) {
    return new IntermediateQueryState(updatedSummary);
  } else {
    if (updatedSummary.result) {
      return updatedSummary.result;
    } else {
      throw new Error(updatedSummary.getErrorMessage() || 'unexpected destination');
    }
  }
}

// Tabs

export interface TabEntry {
  id: string;
  tabName: string;
  query: TalariaQuery;
}
