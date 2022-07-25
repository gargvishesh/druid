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

import { deepDelete, deepSet } from './object-change';

export interface QueryContext {
  useCache?: boolean;
  populateCache?: boolean;
  useApproximateCountDistinct?: boolean;
  useApproximateTopN?: boolean;

  // MSQ
  msqMaxNumTasks?: number;
  msqFinalizeAggregations?: boolean;
  msqDurableShuffleStorage?: boolean;
  maxParseExceptions?: number;
  groupByEnableMultiValueUnnesting?: boolean;

  [key: string]: any;
}

export function isEmptyContext(context: QueryContext): boolean {
  return Object.keys(context).length === 0;
}

// -----------------------------

export function getUseCache(context: QueryContext): boolean {
  const { useCache } = context;
  return typeof useCache === 'boolean' ? useCache : true;
}

export function changeUseCache(context: QueryContext, useCache: boolean): QueryContext {
  let newContext = context;
  if (useCache) {
    newContext = deepDelete(newContext, 'useCache');
    newContext = deepDelete(newContext, 'populateCache');
  } else {
    newContext = deepSet(newContext, 'useCache', false);
    newContext = deepSet(newContext, 'populateCache', false);
  }
  return newContext;
}

// -----------------------------

export function getUseApproximateCountDistinct(context: QueryContext): boolean {
  const { useApproximateCountDistinct } = context;
  return typeof useApproximateCountDistinct === 'boolean' ? useApproximateCountDistinct : true;
}

export function changeUseApproximateCountDistinct(
  context: QueryContext,
  useApproximateCountDistinct: boolean,
): QueryContext {
  if (useApproximateCountDistinct) {
    return deepDelete(context, 'useApproximateCountDistinct');
  } else {
    return deepSet(context, 'useApproximateCountDistinct', false);
  }
}

// -----------------------------

export function getUseApproximateTopN(context: QueryContext): boolean {
  const { useApproximateTopN } = context;
  return typeof useApproximateTopN === 'boolean' ? useApproximateTopN : true;
}

export function changeUseApproximateTopN(
  context: QueryContext,
  useApproximateTopN: boolean,
): QueryContext {
  if (useApproximateTopN) {
    return deepDelete(context, 'useApproximateTopN');
  } else {
    return deepSet(context, 'useApproximateTopN', false);
  }
}

// msqMaxNumTasks

export function getMaxNumTasks(context: QueryContext): number {
  const { msqMaxNumTasks } = context;
  return Math.max(typeof msqMaxNumTasks === 'number' ? msqMaxNumTasks : 0, 2);
}

export function changeMaxNumTasks(
  context: QueryContext,
  maxNumTasks: number | undefined,
): QueryContext {
  return typeof maxNumTasks === 'number'
    ? deepSet(context, 'msqMaxNumTasks', maxNumTasks)
    : deepDelete(context, 'msqMaxNumTasks');
}

// msqFinalizeAggregations

export function getFinalizeAggregations(context: QueryContext): boolean | undefined {
  const { msqFinalizeAggregations } = context;
  return typeof msqFinalizeAggregations === 'boolean' ? msqFinalizeAggregations : undefined;
}

export function changeFinalizeAggregations(
  context: QueryContext,
  finalizeAggregations: boolean | undefined,
): QueryContext {
  return typeof finalizeAggregations === 'boolean'
    ? deepSet(context, 'msqFinalizeAggregations', finalizeAggregations)
    : deepDelete(context, 'msqFinalizeAggregations');
}

// msqFinalizeAggregations

export function getGroupByEnableMultiValueUnnesting(context: QueryContext): boolean | undefined {
  const { groupByEnableMultiValueUnnesting } = context;
  return typeof groupByEnableMultiValueUnnesting === 'boolean'
    ? groupByEnableMultiValueUnnesting
    : undefined;
}

export function changeGroupByEnableMultiValueUnnesting(
  context: QueryContext,
  groupByEnableMultiValueUnnesting: boolean | undefined,
): QueryContext {
  return typeof groupByEnableMultiValueUnnesting === 'boolean'
    ? deepSet(context, 'groupByEnableMultiValueUnnesting', groupByEnableMultiValueUnnesting)
    : deepDelete(context, 'groupByEnableMultiValueUnnesting');
}

// msqDurableShuffleStorage

export function getDurableShuffleStorage(context: QueryContext): boolean {
  const { msqDurableShuffleStorage } = context;
  return Boolean(msqDurableShuffleStorage);
}

export function changeDurableShuffleStorage(
  context: QueryContext,
  msqDurableShuffleStorage: boolean,
): QueryContext {
  if (msqDurableShuffleStorage) {
    return deepSet(context, 'msqDurableShuffleStorage', true);
  } else {
    return deepDelete(context, 'msqDurableShuffleStorage');
  }
}

// maxParseExceptions

export function getMaxParseExceptions(context: QueryContext): number {
  const { maxParseExceptions } = context;
  return Number(maxParseExceptions) || 0;
}

export function changeMaxParseExceptions(
  context: QueryContext,
  maxParseExceptions: number,
): QueryContext {
  if (maxParseExceptions !== 0) {
    return deepSet(context, 'maxParseExceptions', maxParseExceptions);
  } else {
    return deepDelete(context, 'maxParseExceptions');
  }
}
