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
  SqlOrderByExpression,
  SqlQuery,
  SqlRef,
  SqlStar,
  SqlTableRef,
  SqlWithPart,
} from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import { guessDataSourceNameFromInputSource } from '../druid-models/ingestion-spec';
import { InputFormat } from '../druid-models/input-format';
import { InputSource } from '../druid-models/input-source';
import { TIME_COLUMN } from '../druid-models/timestamp-spec';
import { deepDelete, getContextFromSqlQuery } from '../utils';
import { isEmptyContext, QueryContext } from '../utils/query-context';

export const TALARIA_MAX_COLUMNS = 2000;

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
      if ((inputSource as any).files) {
        return (inputSource as any).files[0];
      }
      return `${inputSource.baseDir || '?'}{${inputSource.filter || '?'}}`;

    case 'http':
      return inputSource.uris?.[0] || '?';

    case 's3':
      return (inputSource.uris || inputSource.prefixes)?.[0] || '?';

    case 'hdfs':
      return inputSource.paths?.[0] || '?';

    default:
      return inputSource.type + '()';
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

export function externalConfigToInitQuery(
  externalConfigName: string,
  config: ExternalConfig,
): SqlQuery {
  return SqlQuery.create(SqlTableRef.create(externalConfigName))
    .changeSelectExpressions(
      config.columns.slice(0, TALARIA_MAX_COLUMNS).map(({ name }) => SqlRef.column(name)),
    )
    .changeInsertIntoTable(SqlTableRef.create(externalConfigName));
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

export function externalConfigToIngestQueryPattern(config: ExternalConfig): IngestQueryPattern {
  const hasTimeColumn = false; // ToDo
  return {
    context: hasTimeColumn ? { talariaSegmentGranularity: 'day' } : {},
    insertTableName: guessDataSourceNameFromInputSource(config.inputSource) || 'data',
    mainExternalName: 'ext',
    mainExternalConfig: config,
    filters: [],
    dimensions: config.columns.slice(0, TALARIA_MAX_COLUMNS).map(({ name }) => SqlRef.column(name)),
    partitions: [],
  };
}

// --------------------------------------------

export interface IngestQueryPattern {
  context: QueryContext;
  insertTableName: string;
  mainExternalName: string;
  mainExternalConfig: ExternalConfig;
  filters: readonly SqlExpression[];
  dimensions: readonly SqlExpression[];
  metrics?: readonly SqlExpression[];
  partitions: readonly number[];
}

function verifyHasOutputName(expression: SqlExpression): void {
  if (expression.getOutputName()) return;
  throw new Error(`${expression} must have an AS alias`);
}

export function fitIngestQueryPattern(query: SqlQuery): IngestQueryPattern {
  if (query.explainClause) throw new Error(`Can not use EXPLAIN in the data loader flow`);
  if (query.havingClause) throw new Error(`Can not use HAVING in the data loader flow`);
  if (query.limitClause) throw new Error(`Can not use LIMIT in the data loader flow`);
  if (query.offsetClause) throw new Error(`Can not use OFFSET in the data loader flow`);
  if (query.unionQuery) throw new Error(`Can not use UNION in the data loader flow`);
  if (query.hasStarInSelect()) {
    throw new Error(
      `Can not have * in SELECT in the data loader flow, the columns need to be explicitly listed out`,
    );
  }

  let context = getContextFromSqlQuery(query.toString());

  const insertTableName = query.getInsertIntoTable()?.getTable();
  if (!insertTableName) {
    throw new Error(`Must have an INSERT clause`);
  }

  const withParts = query.getWithParts();
  if (withParts.length !== 1) {
    throw new Error(`Must have exactly one with part`);
  }

  const withPart = withParts[0];
  if (withPart.columns) {
    throw new Error(`Can not have columns in the WITH expression`);
  }

  const mainExternalName = withPart.table.name;
  const mainExternalConfig = fitExternalConfigPattern(withPart.query);

  const filters = query.getWhereExpression()?.decomposeViaAnd() || [];

  let dimensions: readonly SqlExpression[];
  let metrics: readonly SqlExpression[] | undefined;
  if (query.hasGroupBy()) {
    dimensions = query.getGroupedSelectExpressions();
    metrics = query.getAggregateSelectExpressions();
    metrics.forEach(verifyHasOutputName);
  } else {
    dimensions = query.getSelectExpressionsArray();
  }

  dimensions.forEach(verifyHasOutputName);

  // Make sure to adjust talariaSegmentGranularity in the context to the presence of a time column
  if (dimensions.some(d => d.getOutputName() === TIME_COLUMN)) {
    if (!context.talariaSegmentGranularity) {
      context = { ...context, talariaSegmentGranularity: 'day' };
    }
  } else {
    if (context.talariaSegmentGranularity) {
      context = deepDelete(context, 'talariaSegmentGranularity');
    }
  }

  const partitions = query.getOrderByExpressions().map(orderByExpression => {
    if (orderByExpression.direction === 'DESC') {
      throw new Error(`Can not have descending direction in ORDER BY`);
    }

    const selectIndex = query.getSelectIndexForExpression(orderByExpression.expression, true);
    if (selectIndex === -1) {
      throw new Error(
        `Invalid ORDER BY expression ${orderByExpression.expression}, can only partition on a dimension`,
      );
    }
    return selectIndex;
  });

  return {
    context,
    insertTableName,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitions,
  };
}

export function ingestQueryPatternToQuery(
  ingestQueryPattern: IngestQueryPattern,
  preview?: boolean,
): SqlQuery {
  const {
    context,
    insertTableName,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitions,
  } = ingestQueryPattern;

  // ToDo: make this actually work
  const cacheDatasource =
    preview &&
    String(mainExternalConfig.inputSource.uris) ===
      'https://static.imply.io/data/kttm/kttm-2019-08-25.json.gz_'
      ? '_tmp_loader_cache_20210101_010102'
      : undefined;

  return SqlQuery.from(SqlTableRef.create(mainExternalName))
    .applyIf(!isEmptyContext(context) && !preview, q =>
      q.changeSpace('initial', `--:context ${JSONBig.stringify(context)}\n`),
    )
    .applyIf(!preview, q => q.changeInsertIntoTable(insertTableName))
    .changeWithParts([
      SqlWithPart.simple(
        mainExternalName,
        cacheDatasource
          ? SqlQuery.create(SqlTableRef.create(cacheDatasource))
          : SqlQuery.create(externalConfigToTableExpression(mainExternalConfig)).applyIf(
              preview,
              q => q.changeLimitValue(10000),
            ),
      ),
    ])
    .applyForEach(dimensions, (query, ex) =>
      query.addSelect(ex, metrics ? { addToGroupBy: 'end' } : {}),
    )
    .applyForEach(metrics || [], (query, ex) => query.addSelect(ex))
    .applyIf(filters.length, query => query.changeWhereExpression(SqlExpression.and(...filters)))
    .applyIf(!preview, q =>
      q.changeOrderByExpressions(partitions.map(p => SqlOrderByExpression.index(p))),
    );
}
