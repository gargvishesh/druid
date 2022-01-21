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
import { QueryResult, RefName, SqlLiteral, SqlRef, SqlTableRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import {
  DimensionSpec,
  inflateDimensionSpec,
  IngestionSpec,
  MetricSpec,
  TimestampSpec,
  Transform,
} from '../../druid-models';
import { Api } from '../../singletons';
import { TalariaQuery, TalariaSummary } from '../../talaria-models';
import {
  deepGet,
  downloadHref,
  DruidError,
  filterMap,
  IntermediateQueryState,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { QueryContext } from '../../utils/query-context';

export interface SubmitAsyncQueryOptions {
  query: string | Record<string, any>;
  context: QueryContext;
  prefixLines?: number;
  cancelToken?: CancelToken;
}

export async function submitAsyncQuery(options: SubmitAsyncQueryOptions): Promise<TalariaSummary> {
  const { query, context, prefixLines, cancelToken } = options;

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

  return TalariaSummary.fromAsyncStatus(asyncResp.data, sqlQuery, context);
}

export async function getTalariaDetailSummary(
  id: string,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  const resp = await Api.instance.get(`/druid/v2/sql/async/${Api.encodePath(id)}`, {
    cancelToken,
  });

  return TalariaSummary.fromDetail(resp.data);
}

export async function updateSummaryWithAsyncIfNeeded(
  currentSummary: TalariaSummary,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (!currentSummary.isWaitingForQuery()) return currentSummary;

  let newSummary: TalariaSummary;
  try {
    newSummary = await getTalariaDetailSummary(currentSummary.id);
  } catch {
    const resp = await Api.instance.get(
      `/druid/v2/sql/async/${Api.encodePath(currentSummary.id)}/status`,
      { cancelToken },
    );

    newSummary = TalariaSummary.fromAsyncStatus(resp.data);
  }

  return currentSummary.updateWith(newSummary);
}

async function updateSummaryWithResultsIfNeeded(
  currentSummary: TalariaSummary,
  cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (
    currentSummary.status !== 'SUCCESS' ||
    currentSummary.result ||
    currentSummary.destination?.type !== 'taskReport'
  ) {
    return currentSummary;
  }

  const results = await getAsyncResult(currentSummary.id, cancelToken);

  return currentSummary.changeResult(results);
}

async function updateSummaryWithDatasourceExistsIfNeeded(
  currentSummary: TalariaSummary,
  _cancelToken?: CancelToken,
): Promise<TalariaSummary> {
  if (
    !(currentSummary.destination?.type === 'dataSource' && !currentSummary.destination.exists) ||
    currentSummary.status !== 'SUCCESS'
  ) {
    return currentSummary;
  }

  // // Get the table to see if it exists
  // const tableCheck = await queryDruidSql({
  //   query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ${SqlLiteral.create(
  //     currentSummary.destination.dataSource,
  //   )}`,
  // });
  //
  // if (!tableCheck.length) return currentSummary;
  // return currentSummary.markDestinationDatasourceExists();

  // Get the segments to see if it is available
  const segmentCheck = await queryDruidSql({
    query: `SELECT segment_id FROM sys.segments WHERE datasource = ${SqlLiteral.create(
      currentSummary.destination.dataSource,
    )} AND is_published = 1 AND is_overshadowed = 0 AND is_available = 0 LIMIT 1`,
  });

  if (segmentCheck.length) return currentSummary;
  return currentSummary.markDestinationDatasourceExists();
}

export function cancelAsyncQueryOnCancel(
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

export function downloadResults(id: string, filename: string): void {
  downloadHref({
    href: `/druid/v2/sql/async/${Api.encodePath(id)}/results`,
    filename: filename,
  });
}

export async function talariaBackgroundStatusCheck(
  currentSummary: TalariaSummary,
  _query: any,
  cancelToken: CancelToken,
) {
  currentSummary = await updateSummaryWithAsyncIfNeeded(currentSummary, cancelToken);

  if (currentSummary.isWaitingForQuery()) return new IntermediateQueryState(currentSummary);

  currentSummary = await updateSummaryWithResultsIfNeeded(currentSummary, cancelToken);
  currentSummary = await updateSummaryWithDatasourceExistsIfNeeded(currentSummary, cancelToken);

  if (!currentSummary.isFullyComplete()) return new IntermediateQueryState(currentSummary);

  return currentSummary;
}

export async function talariaBackgroundResultStatusCheck(
  currentSummary: TalariaSummary,
  query: any,
  cancelToken: CancelToken,
) {
  const updatedSummary = await talariaBackgroundStatusCheck(currentSummary, query, cancelToken);
  if (updatedSummary instanceof IntermediateQueryState) return updatedSummary;

  if (updatedSummary.result) {
    return updatedSummary.result;
  } else {
    throw new Error(updatedSummary.getErrorMessage() || 'unexpected destination');
  }
}

// Tabs

export interface TabEntry {
  id: string;
  tabName: string;
  query: TalariaQuery;
}

// Spec conversion

export function convertSpecToSql(spec: IngestionSpec): string {
  if (spec.type !== 'index_parallel') throw new Error('only index_parallel is supported');

  const lines: string[] = [];

  const rollup = deepGet(spec, 'spec.dataSchema.granularitySpec.rollup') ?? true;

  const timestampSpec: TimestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec');
  if (!timestampSpec) throw new Error(`spec.dataSchema.timestampSpec is not defined`);

  let dimensions = deepGet(spec, 'spec.dataSchema.dimensionsSpec.dimensions');
  if (!Array.isArray(dimensions)) {
    throw new Error(`spec.dataSchema.dimensionsSpec.dimensions must be an array`);
  }
  dimensions = dimensions.map(inflateDimensionSpec);

  let columns = dimensions.map((d: DimensionSpec) => ({
    name: d.name,
    type: d.type,
  }));

  const metricsSpec = deepGet(spec, 'spec.dataSchema.metricsSpec');
  if (Array.isArray(metricsSpec)) {
    columns = columns.concat(
      filterMap(metricsSpec, metricSpec =>
        metricSpec.fieldName
          ? {
              name: metricSpec.fieldName,
              type: metricSpecTypeToDataType(metricSpec.type),
            }
          : undefined,
      ),
    );
  }

  let timeExpression: string;
  const column = timestampSpec.column || 'timestamp';
  const format = timestampSpec.format || 'auto';
  switch (format) {
    case 'auto':
    case 'iso':
      columns.unshift({ name: column, type: 'string' });
      timeExpression = `TIME_PARSE(${SqlRef.column(column)})`;
      break;

    case 'posix':
      columns.unshift({ name: column, type: 'long' });
      timeExpression = `MILLIS_TO_TIMESTAMP(${SqlRef.column(column)} * 1000)`;
      break;

    case 'millis':
      columns.unshift({ name: column, type: 'long' });
      timeExpression = `MILLIS_TO_TIMESTAMP(${SqlRef.column(column)})`;
      break;

    case 'micro':
      columns.unshift({ name: column, type: 'long' });
      timeExpression = `MILLIS_TO_TIMESTAMP(${SqlRef.column(column)} / 1000)`;
      break;

    case 'nano':
      columns.unshift({ name: column, type: 'long' });
      timeExpression = `MILLIS_TO_TIMESTAMP(${SqlRef.column(column)} / 1000000)`;
      break;

    default:
      columns.unshift({ name: column, type: 'string' });
      timeExpression = `TIME_PARSE(${SqlRef.column(column)}, ${SqlLiteral.create(format)})`;
      break;
  }

  if (timestampSpec.missingValue) {
    timeExpression = `COALESCE(${timeExpression}, TIME_PARSE(${SqlLiteral.create(
      timestampSpec.missingValue,
    )}))`;
  }

  timeExpression = convertQueryGranularity(
    timeExpression,
    deepGet(spec, 'spec.dataSchema.granularitySpec.queryGranularity'),
  );

  lines.push(`-- This SQL query was auto generated from an ingestion spec`);

  if (!deepGet(spec, 'spec.ioConfig.appendToExisting')) {
    lines.push(`--:context talariaReplaceTimeChunks: all`);
  }
  if (deepGet(spec, 'spec.ioConfig.dropExisting')) {
    lines.push(
      `-- spec.ioConfig.dropExisting was set but not converted, 'intervals' was set to ${JSONBig.stringify(
        deepGet(spec, 'spec.dataSchema.granularitySpec.intervals'),
      )}`,
    );
  }

  const segmentGranularity = deepGet(spec, 'spec.dataSchema.granularitySpec.segmentGranularity');
  if (typeof segmentGranularity !== 'string') {
    throw new Error(`spec.dataSchema.granularitySpec.segmentGranularity is not a string`);
  }
  lines.push(`--:context talariaSegmentGranularity: ${segmentGranularity.toLowerCase()}`);

  if (rollup) {
    lines.push(`--:context talariaFinalizeAggregations: false`);
  }

  const dataSource = deepGet(spec, 'spec.dataSchema.dataSource');
  if (typeof dataSource !== 'string') throw new Error(`spec.dataSchema.dataSource is not a string`);
  lines.push(`INSERT INTO ${SqlTableRef.create(dataSource)}`);

  lines.push(`WITH "external_data" AS (SELECT * FROM TABLE(`);
  lines.push(`  EXTERN(`);

  const inputSource = deepGet(spec, 'spec.ioConfig.inputSource');
  if (!inputSource) throw new Error(`spec.ioConfig.inputSource is not defined`);
  lines.push(`    ${SqlLiteral.create(JSONBig.stringify(inputSource))},`);

  const inputFormat = deepGet(spec, 'spec.ioConfig.inputFormat');
  if (!inputFormat) throw new Error(`spec.ioConfig.inputFormat is not defined`);
  lines.push(`    ${SqlLiteral.create(JSONBig.stringify(inputFormat))},`);

  lines.push(`    ${SqlLiteral.create(JSONBig.stringify(columns))}`);
  lines.push(`  )`);
  lines.push(`))`);

  lines.push(`SELECT`);

  const transforms: Transform[] | undefined = deepGet(
    spec,
    'spec.dataSchema.transformSpec.transforms',
  );
  if (Array.isArray(transforms) && transforms.length) {
    lines.push(
      `-- The spec contained transforms that could not be automatically converted: ${JSONBig.stringify(
        transforms,
      )}`,
    );
  }

  const dimensionExpressions = [`  ${timeExpression} AS __time`].concat(
    dimensions.map((dimension: DimensionSpec) => `  ${SqlRef.columnWithQuotes(dimension.name)}`),
  );

  const aggExpressions = Array.isArray(metricsSpec)
    ? metricsSpec.map(metricSpec => '  ' + metricSpecToSelect(metricSpec))
    : [];

  lines.push(dimensionExpressions.concat(aggExpressions).join(',\n'));

  lines.push(`FROM "external_data"`);

  const filter = deepGet(spec, 'spec.dataSchema.transformSpec.filter');
  if (filter) {
    lines.push(
      `-- The spec contained a filter that could not be automatically converted: ${JSONBig.stringify(
        filter,
      )}`,
    );
  }

  if (rollup) {
    lines.push(`GROUP BY ${dimensionExpressions.map((_, i) => i + 1).join(', ')}`);
  }

  const partitionsSpec = deepGet(spec, 'spec.tuningConfig.partitionsSpec') || {};
  const partitionDimensions =
    partitionsSpec.partitionDimensions ||
    (partitionsSpec.partitionDimension ? [partitionsSpec.partitionDimension] : undefined);
  if (Array.isArray(partitionDimensions)) {
    lines.push(`ORDER BY ${partitionDimensions.map(d => SqlRef.columnWithQuotes(d)).join(', ')}`);
  }

  return lines.join('\n');
}

function convertQueryGranularity(
  timeExpression: string,
  queryGranularity: { type: string } | string | undefined,
) {
  if (!queryGranularity) return timeExpression;

  const effectiveQueryGranularity =
    typeof queryGranularity === 'string'
      ? queryGranularity
      : typeof queryGranularity.type === 'string'
      ? queryGranularity.type
      : undefined;

  if (effectiveQueryGranularity) {
    const queryGranularitySql = QUERY_GRANULARITY_MAP[effectiveQueryGranularity.toLowerCase()];

    if (queryGranularitySql) {
      return queryGranularitySql.replace('?', timeExpression);
    }
  }

  throw new Error(`spec.dataSchema.granularitySpec.queryGranularity is not recognized`);
}

const QUERY_GRANULARITY_MAP: Record<string, string> = {
  none: `?`,
  second: `TIME_FLOOR(?, 'PT1S')`,
  minute: `TIME_FLOOR(?, 'PT1M')`,
  fifteen_minute: `TIME_FLOOR(?, 'PT15M')`,
  thirty_minute: `TIME_FLOOR(?, 'PT30M')`,
  hour: `TIME_FLOOR(?, 'PT1H')`,
  day: `TIME_FLOOR(?, 'P1D')`,
  week: `TIME_FLOOR(?, 'P7D')`,
  month: `TIME_FLOOR(?, 'P1M')`,
  quarter: `TIME_FLOOR(?, 'P3M')`,
  year: `TIME_FLOOR(?, 'P1Y')`,
};

function metricSpecTypeToDataType(metricSpecType: string): string {
  const m = /^(long|float|double|string)/.exec(String(metricSpecType));
  if (m) return m[1];

  switch (metricSpecType) {
    case 'thetaSketch':
    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
    case 'quantilesDoublesSketch':
    case 'momentSketch':
    case 'fixedBucketsHistogram':
    case 'hyperUnique':
      return 'string';
  }

  return 'double';
}

function metricSpecToSelect(metricSpec: MetricSpec): string {
  const name = metricSpec.name;
  const expression = metricSpecToSqlExpression(metricSpec);
  if (!name || !expression) {
    return `-- could not convert metric: ${JSONBig.stringify(metricSpec)}`;
  }

  return `${expression} AS ${RefName.create(name, true)}`;
}

function metricSpecToSqlExpression(metricSpec: MetricSpec): string | undefined {
  if (metricSpec.type === 'count') {
    return `COUNT(*)`; // count is special as it does not have a fieldName
  }

  if (!metricSpec.fieldName) return;
  const ref = SqlRef.columnWithQuotes(metricSpec.fieldName);

  switch (metricSpec.type) {
    case 'longSum':
    case 'floatSum':
    case 'doubleSum':
      return `SUM(${ref})`;

    case 'longMin':
    case 'floatMin':
    case 'doubleMin':
      return `MIN(${ref})`;

    case 'longMax':
    case 'floatMax':
    case 'doubleMax':
      return `MAX(${ref})`;

    case 'doubleFirst':
    case 'floatFirst':
    case 'longFirst':
      return `EARLIEST(${ref})`;

    case 'stringFirst':
      return `EARLIEST(${ref}, ${SqlLiteral.create(metricSpec.maxStringBytes || 128)})`;

    case 'doubleLast':
    case 'floatLast':
    case 'longLast':
      return `LATEST(${ref})`;

    case 'stringLast':
      return `LATEST(${ref}, ${SqlLiteral.create(metricSpec.maxStringBytes || 128)})`;

    case 'thetaSketch':
      return `APPROX_COUNT_DISTINCT_DS_THETA(${ref}${extraArgs([metricSpec.size, 16384])})`;

    case 'HLLSketchBuild':
    case 'HLLSketchMerge':
      return `APPROX_COUNT_DISTINCT_DS_HLL(${ref}${extraArgs(
        [metricSpec.lgK, 12],
        [metricSpec.tgtHllType, 'HLL_4'],
      )})`;

    case 'quantilesDoublesSketch':
      // For consistency with the above this should be APPROX_QUANTILE_DS but that requires a post agg so it does not work quite right.
      return `DS_QUANTILES_SKETCH(${ref}${extraArgs([metricSpec.k, 128])})`;

    case 'hyperUnique':
      return `APPROX_COUNT_DISTINCT_BUILTIN(${ref})`;

    default:
      // The following things are (knowingly) not supported:
      // tDigestSketch, momentSketch, fixedBucketsHistogram
      return;
  }
}

function extraArgs(...thingAndDefaults: [any, any?][]): string {
  while (
    thingAndDefaults.length &&
    typeof thingAndDefaults[thingAndDefaults.length - 1][0] === 'undefined'
  ) {
    thingAndDefaults.pop();
  }

  if (!thingAndDefaults.length) return '';
  return ', ' + thingAndDefaults.map(([x, def]) => SqlLiteral.create(x ?? def)).join(', ');
}
