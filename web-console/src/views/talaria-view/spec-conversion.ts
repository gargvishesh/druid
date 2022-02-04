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

import { RefName, SqlLiteral, SqlRef, SqlTableRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import {
  DimensionSpec,
  inflateDimensionSpec,
  IngestionSpec,
  MetricSpec,
  TimestampSpec,
  Transform,
} from '../../druid-models';
import { deepGet, filterMap } from '../../utils';

const EXTERN_NAME = SqlTableRef.create('ioConfigExtern');
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

  lines.push(`WITH ${EXTERN_NAME} AS (SELECT * FROM TABLE(`);
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

  const transforms: Transform[] = deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || [];
  if (!Array.isArray(transforms))
    throw new Error(`spec.dataSchema.transformSpec.transforms is not an array`);
  if (transforms.length) {
    lines.push(`  -- The spec contained transforms that could not be automatically converted.`);
  }

  const dimensionExpressions = [`  ${timeExpression} AS __time,`].concat(
    dimensions.flatMap((dimension: DimensionSpec) => {
      const dimensionName = dimension.name;
      const relevantTransform = transforms.find(t => t.name === dimensionName);
      return `  ${SqlRef.columnWithQuotes(dimensionName)},${
        relevantTransform ? ` -- Relevant transform: ${JSONBig.stringify(relevantTransform)}` : ''
      }`;
    }),
  );

  const selectExpressions = dimensionExpressions.concat(
    Array.isArray(metricsSpec)
      ? metricsSpec.map(metricSpec => `  ${metricSpecToSelect(metricSpec)},`)
      : [],
  );

  // Remove trailing comma from last expression
  selectExpressions[selectExpressions.length - 1] = selectExpressions[selectExpressions.length - 1]
    .replace(/,$/, '')
    .replace(/,(\s+--)/, '$1');

  lines.push(selectExpressions.join('\n'));
  lines.push(`FROM ${EXTERN_NAME}`);

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
