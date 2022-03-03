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
  SqlStar,
  SqlTableRef,
  SqlWithPart,
} from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import { guessDataSourceNameFromInputSource } from '../druid-models/ingestion-spec';
import { InputFormat } from '../druid-models/input-format';
import { InputSource } from '../druid-models/input-source';
import { filterMap, getContextFromSqlQuery, oneOf } from '../utils';

function isStringArray(a: any): a is string[] {
  return Array.isArray(a) && a.every(x => typeof x === 'string');
}

export const TALARIA_MAX_COLUMNS = 2000;

const TALARIA_REPLACE_TIME_CHUNKS = 'talariaReplaceTimeChunks';
const TALARIA_FINALIZE_AGGREGATIONS = 'talariaFinalizeAggregations';

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

const INITIAL_CONTEXT_LINES = [
  `--:context talariaReplaceTimeChunks: all`,
  `--:context talariaFinalizeAggregations: false`,
];

export function externalConfigToInitQuery(
  externalConfigName: string,
  config: ExternalConfig,
): SqlQuery {
  return SqlQuery.create(SqlTableRef.create(externalConfigName))
    .changeSpace('initial', INITIAL_CONTEXT_LINES.join('\n') + '\n')
    .changeSelectExpressions(
      config.columns.slice(0, TALARIA_MAX_COLUMNS).map(({ name }) => SqlRef.column(name)),
    )
    .changeInsertIntoTable(SqlTableRef.create(externalConfigName))
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

export function externalConfigToIngestQueryPattern(config: ExternalConfig): IngestQueryPattern {
  const hasTimeColumn = false; // ToDo: actually detect the time column
  return {
    destinationTableName: guessDataSourceNameFromInputSource(config.inputSource) || 'data',
    replaceTimeChunks: 'all',
    mainExternalName: 'ext',
    mainExternalConfig: config,
    filters: [],
    dimensions: config.columns.slice(0, TALARIA_MAX_COLUMNS).map(({ name }) => SqlRef.column(name)),
    partitionedBy: hasTimeColumn ? 'day' : 'all',
    clusteredBy: [],
  };
}

// --------------------------------------------

export interface IngestQueryPattern {
  destinationTableName: string;
  replaceTimeChunks?: 'all' | string[];
  mainExternalName: string;
  mainExternalConfig: ExternalConfig;
  filters: readonly SqlExpression[];
  dimensions: readonly SqlExpression[];
  metrics?: readonly SqlExpression[];
  partitionedBy: string;
  clusteredBy: readonly number[];
}

export function getQueryPatternExpression(
  pattern: IngestQueryPattern,
  index: number,
): SqlExpression | undefined {
  const { dimensions, metrics } = pattern;
  if (index < dimensions.length) {
    return dimensions[index];
  } else if (metrics) {
    return metrics[index - dimensions.length];
  }
  return;
}

export function getQueryPatternExpressionType(
  pattern: IngestQueryPattern,
  index: number,
): 'dimension' | 'metric' | undefined {
  const { dimensions, metrics } = pattern;
  if (index < dimensions.length) {
    return 'dimension';
  } else if (metrics) {
    return 'metric';
  }
  return;
}

export function changeQueryPatternExpression(
  pattern: IngestQueryPattern,
  index: number,
  type: 'dimension' | 'metric',
  ex: SqlExpression | undefined,
): IngestQueryPattern {
  let { dimensions, metrics } = pattern;
  if (index === -1) {
    if (ex) {
      if (type === 'dimension') {
        dimensions = dimensions.concat(ex);
      } else if (metrics) {
        metrics = metrics.concat(ex);
      }
    }
  } else if (index < dimensions.length) {
    dimensions = filterMap(dimensions, (d, i) => (i === index ? ex : d));
  } else if (metrics) {
    const metricIndex = index - dimensions.length;
    metrics = filterMap(metrics, (m, i) => (i === metricIndex ? ex : m));
  }
  return { ...pattern, dimensions, metrics };
}

function verifyHasOutputName(expression: SqlExpression): void {
  if (expression.getOutputName()) return;
  throw new Error(`${expression} must have an AS alias`);
}

export function fitIngestQueryPattern(query: SqlQuery): IngestQueryPattern {
  if (query.explainClause) throw new Error(`Can not use EXPLAIN in the data loader flow`);
  if (query.havingClause) throw new Error(`Can not use HAVING in the data loader flow`);
  if (query.orderByClause) throw new Error(`Can not USE ORDER BY in the data loader flow`);
  if (query.limitClause) throw new Error(`Can not use LIMIT in the data loader flow`);
  if (query.offsetClause) throw new Error(`Can not use OFFSET in the data loader flow`);
  if (query.unionQuery) throw new Error(`Can not use UNION in the data loader flow`);

  if (query.hasStarInSelect()) {
    throw new Error(
      `Can not have * in SELECT in the data loader flow, the columns need to be explicitly listed out`,
    );
  }

  const destinationTableName = query.getInsertIntoTable()?.getTable();
  if (!destinationTableName) {
    throw new Error(`Must have an INSERT clause`);
  }

  const context = getContextFromSqlQuery(query.toString());
  const replaceTimeChunks = context[TALARIA_REPLACE_TIME_CHUNKS];
  if (replaceTimeChunks && replaceTimeChunks !== 'all' && !isStringArray(replaceTimeChunks)) {
    throw new Error(`${TALARIA_REPLACE_TIME_CHUNKS} must be 'all' or and array`);
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

  const partitionedByClause = query.partitionedByClause;
  if (!partitionedByClause) {
    throw new Error(`Must have a PARTITIONED BY clause`);
  }
  let partitionedBy: string;
  if (partitionedByClause.expression) {
    if (partitionedByClause.expression instanceof SqlLiteral) {
      partitionedBy = String(partitionedByClause.expression.value).toLowerCase();
    } else {
      partitionedBy = '';
    }
  } else {
    partitionedBy = 'all';
  }
  if (!oneOf(partitionedBy, 'hour', 'day', 'month', 'year', 'all')) {
    throw new Error(`Must partition by HOUR, DAY, MONTH, YEAR, or ALL TIME`);
  }

  const clusteredByExpressions = query.clusteredByClause
    ? query.clusteredByClause.expressions.values
    : [];
  const clusteredBy = clusteredByExpressions.map(clusteredByExpression => {
    const selectIndex = query.getSelectIndexForExpression(clusteredByExpression, true);
    if (selectIndex === -1) {
      throw new Error(
        `Invalid CLUSTERED BY expression ${clusteredByExpression}, can only partition on a dimension`,
      );
    }
    return selectIndex;
  });

  return {
    destinationTableName,
    replaceTimeChunks,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitionedBy,
    clusteredBy,
  };
}

export function ingestQueryPatternToQuery(
  ingestQueryPattern: IngestQueryPattern,
  preview?: boolean,
  sampleDatasource?: string,
): SqlQuery {
  const {
    destinationTableName,
    replaceTimeChunks,
    mainExternalName,
    mainExternalConfig,
    filters,
    dimensions,
    metrics,
    partitionedBy,
    clusteredBy,
  } = ingestQueryPattern;

  const initialContextLines: string[] = [];
  if (replaceTimeChunks) {
    initialContextLines.push(
      `--:context ${TALARIA_REPLACE_TIME_CHUNKS}: ${
        replaceTimeChunks === 'all' ? 'all' : JSONBig.stringify(replaceTimeChunks)
      }`,
    );
  }
  if (metrics) {
    initialContextLines.push(`--:context ${TALARIA_FINALIZE_AGGREGATIONS}: false`);
  }

  return SqlQuery.from(SqlTableRef.create(mainExternalName))
    .applyIf(initialContextLines.length, q =>
      q.changeSpace('initial', initialContextLines.join('\n') + '\n'),
    )
    .applyIf(!preview, q => q.changeInsertIntoTable(destinationTableName))
    .changeWithParts([
      SqlWithPart.simple(
        mainExternalName,
        sampleDatasource
          ? SqlQuery.create(SqlTableRef.create(sampleDatasource))
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
      q
        .changePartitionedByClause(
          SqlPartitionedByClause.create(
            partitionedBy !== 'all'
              ? new SqlLiteral({
                  value: partitionedBy.toUpperCase(),
                  stringValue: partitionedBy.toUpperCase(),
                })
              : undefined,
          ),
        )
        .changeClusteredByExpressions(clusteredBy.map(p => SqlLiteral.index(p))),
    );
}

export function ingestQueryPatternToSampleQuery(
  ingestQueryPattern: IngestQueryPattern,
  sampleName: string,
): SqlQuery {
  const { mainExternalConfig } = ingestQueryPattern;

  return SqlQuery.create(externalConfigToTableExpression(mainExternalConfig))
    .changeInsertIntoTable(sampleName)
    .changePartitionedByClause(SqlPartitionedByClause.create(undefined))
    .changeLimitValue(10000);
}
