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

import {
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlPartitionedByClause,
  SqlQuery,
  SqlRef,
  SqlReplaceClause,
  SqlStar,
  SqlTableRef,
} from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import { InputFormat, InputSource } from '../druid-models';
import { nonEmptyArray } from '../utils';

export const MULTI_STAGE_QUERY_MAX_COLUMNS = 2000;

export interface ExternalConfig {
  inputSource: InputSource;
  inputFormat: InputFormat;
  columns: ExternalConfigColumn[];
}

export interface ExternalConfigColumn {
  name: string;
  type: string;
}

export function summarizeInputSource(inputSource: InputSource): string {
  switch (inputSource.type) {
    case 'inline':
      return `inline(...)`;

    case 'local':
      // ToDo: make this official
      if (nonEmptyArray((inputSource as any).files)) {
        return (inputSource as any).files[0];
      }
      return `${inputSource.baseDir || '?'}{${inputSource.filter || '?'}}`;

    case 'http':
      return inputSource.uris?.[0] || '?';

    case 's3':
    case 'google':
      return (inputSource.uris || inputSource.prefixes)?.[0] || '?';

    case 'hdfs':
      return inputSource.paths?.[0] || '?';

    default:
      return inputSource.type + '(...)';
  }
}

export function summarizeInputFormat(inputFormat: InputFormat): string {
  return String(inputFormat.type);
}

export function summarizeExternalConfig(externalConfig: ExternalConfig): string {
  return `${summarizeInputSource(externalConfig.inputSource)} [${summarizeInputFormat(
    externalConfig.inputFormat,
  )}]`;
}

export function externalConfigToTableExpression(config: ExternalConfig): SqlExpression {
  return SqlExpression.parse(`TABLE(
  EXTERN(
    ${SqlLiteral.create(JSONBig.stringify(config.inputSource))},
    ${SqlLiteral.create(JSONBig.stringify(config.inputFormat))},
    ${SqlLiteral.create(JSONBig.stringify(config.columns))}
  )
)`);
}

const INITIAL_CONTEXT_LINES = [
  `--:context msqFinalizeAggregations: false`,
  `--:context groupByEnableMultiValueUnnesting: false`,
];

export function externalConfigToInitQuery(
  externalConfigName: string,
  config: ExternalConfig,
  isArrays: boolean[],
): SqlQuery {
  return SqlQuery.create(SqlTableRef.create(externalConfigName))
    .changeSpace('initial', INITIAL_CONTEXT_LINES.join('\n') + '\n')
    .changeSelectExpressions(
      config.columns
        .slice(0, MULTI_STAGE_QUERY_MAX_COLUMNS)
        .map(({ name }, i) =>
          SqlRef.column(name).applyIf(
            isArrays[i],
            ex => SqlFunction.simple('MV_TO_ARRAY', [ex]).as(name) as any,
          ),
        ),
    )
    .changeReplaceClause(SqlReplaceClause.create(externalConfigName))
    .changePartitionedByClause(SqlPartitionedByClause.create(undefined));
}

export function fitExternalConfigPattern(query: SqlQuery): ExternalConfig {
  if (!(query.getSelectExpressionForIndex(0) instanceof SqlStar)) {
    throw new Error(`External SELECT must only be a star`);
  }

  const tableFn = query.fromClause?.expressions?.first();
  if (!(tableFn instanceof SqlFunction) || tableFn.functionName !== 'TABLE') {
    throw new Error(`External FROM must be a TABLE function`);
  }

  const externFn = tableFn.getArg(0);
  if (!(externFn instanceof SqlFunction) || externFn.functionName !== 'EXTERN') {
    throw new Error(`Within the TABLE function there must be an extern function`);
  }

  let inputSource: any;
  try {
    const arg0 = externFn.getArg(0);
    inputSource = JSONBig.parse(arg0 instanceof SqlLiteral ? String(arg0.value) : '#');
  } catch {
    throw new Error(`The first argument to the extern function must be a string embedding JSON`);
  }

  let inputFormat: any;
  try {
    const arg1 = externFn.getArg(1);
    inputFormat = JSONBig.parse(arg1 instanceof SqlLiteral ? String(arg1.value) : '#');
  } catch {
    throw new Error(`The second argument to the extern function must be a string embedding JSON`);
  }

  let columns: any;
  try {
    const arg2 = externFn.getArg(2);
    columns = JSONBig.parse(arg2 instanceof SqlLiteral ? String(arg2.value) : '#');
  } catch {
    throw new Error(`The third argument to the extern function must be a string embedding JSON`);
  }

  return {
    inputSource,
    inputFormat,
    columns,
  };
}
