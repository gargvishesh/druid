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

import { Button, Callout, Intent, Menu, Tag } from '@blueprintjs/core';
import { Popover2 } from '@blueprintjs/popover2';
import { CancelToken } from 'axios';
import { QueryResult, SqlExpression, SqlFunction, SqlQuery } from 'druid-query-toolkit';
import React, { useEffect } from 'react';

import { useQueryManager } from '../../../../hooks';
import { QueryExecution } from '../../../../talaria-models';
import {
  filterMap,
  formatPercentClapped,
  IntermediateQueryState,
  QueryAction,
} from '../../../../utils';
import { ColumnActionMenu } from '../../column-action-menu/column-action-menu';
import { submitAsyncQuery, talariaBackgroundStatusCheck } from '../../execution-utils';

import './rollup-analysis-pane.scss';

const CAST_AS_VARCHAR_TEMPLATE = SqlExpression.parse(`NVL(CAST(? AS VARCHAR), '__NULL__')`);

function expressionCastToString(ex: SqlExpression): SqlExpression {
  if (ex instanceof SqlFunction && ex.getEffectiveFunctionName() === 'TIME_PARSE') {
    ex = SqlFunction.simple('TIMESTAMP_TO_MILLIS', [ex]);
  }
  return CAST_AS_VARCHAR_TEMPLATE.fillPlaceholders([ex]);
}

function countDistinct(ex: SqlExpression): SqlExpression {
  return SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_HLL', [ex]);
}

function within(a: number, b: number, percent: number): boolean {
  return Math.abs(a - b) / Math.abs(Math.min(a, b)) < percent;
}

function pairs(n: number): [number, number][] {
  const p: [number, number][] = [];
  for (let i = 0; i < n - 1; i++) {
    for (let j = i + 1; j < n; j++) {
      p.push([i, j]);
    }
  }
  return p;
}

interface AnalyzeQuery {
  expressions: readonly SqlExpression[];
  deep: boolean;
}

interface AnalyzeResult {
  deep: boolean;
  count: number;
  overall: number;
  counts: number[];
  pairCounts: number[];
  implications: Implication[];
}

function queryResultToAnalysis(
  analyzeQuery: AnalyzeQuery,
  queryResult: QueryResult,
): AnalyzeResult {
  const row = queryResult.rows[0];
  const numExpressions = analyzeQuery.expressions.length;

  const counts = row.slice(2, numExpressions + 2);
  const pairCounts = row.slice(numExpressions + 2);
  return {
    deep: analyzeQuery.deep,
    count: row[0],
    overall: row[1],
    counts,
    pairCounts,
    implications: getImplications(counts, pairCounts),
  };
}

type ImplicationType = 'imply' | 'equivalent';

interface Implication {
  type: ImplicationType;
  a: number;
  b: number;
}

function getImplications(counts: number[], pairCounts: number[]): Implication[] {
  const pairIndexes = pairs(counts.length);

  return filterMap(pairIndexes, ([i, j], index) => {
    const pairCount = pairCounts[index];
    if (counts[i] < 2) return;
    if (counts[j] < 2) return;
    const iImplyJ = within(counts[i], pairCount, 0.01);
    const jImplyI = within(counts[j], pairCount, 0.01);

    if (iImplyJ && jImplyI) {
      return { type: 'equivalent' as ImplicationType, a: i, b: j };
    } else if (iImplyJ) {
      return { type: 'imply' as ImplicationType, a: i, b: j };
    } else if (jImplyI) {
      return { type: 'imply' as ImplicationType, a: j, b: i };
    }

    return;
  }).sort((x, y) => {
    const diffA = x.a - y.a;
    if (diffA !== 0) return diffA;
    return x.b - y.b;
  });
}

interface RollupAnalysisPaneProps {
  dimensions: readonly SqlExpression[];
  seedQuery: SqlQuery;
  queryResult: QueryResult | undefined;
  onEditColumn(expression: number): void;
  onQueryAction(action: QueryAction): void;
  onClose(): void;
}

export const RollupAnalysisPane = React.memo(function RollupAnalysisPane(
  props: RollupAnalysisPaneProps,
) {
  const { dimensions, seedQuery, queryResult, onEditColumn, onQueryAction, onClose } = props;

  const [analyzeQueryState, analyzeQueryManager] = useQueryManager<
    AnalyzeQuery,
    AnalyzeResult,
    QueryExecution
  >({
    processQuery: async (analyzeQuery: AnalyzeQuery, cancelToken) => {
      const { expressions, deep } = analyzeQuery;

      const groupedAsStrings = expressions.map(ex =>
        expressionCastToString(ex.getUnderlyingExpression()),
      );

      const expressionPairs: SqlExpression[] = deep
        ? pairs(expressions.length).map(([i, j]) =>
            countDistinct(SqlFunction.simple('CONCAT', [groupedAsStrings[i], groupedAsStrings[j]])),
          )
        : [];

      const queryString = seedQuery
        .changeSelectExpressions(groupedAsStrings.map(countDistinct).concat(expressionPairs))
        .addSelect(countDistinct(SqlFunction.simple('CONCAT', groupedAsStrings)), {
          insertIndex: 0,
        })
        .addSelect(SqlFunction.COUNT_STAR, { insertIndex: 0 })
        .changeGroupByExpressions([])
        .changeHavingClause(undefined)
        .changeOrderByClause(undefined)
        .toString();

      // console.log('analyze:', queryString);

      const res = await submitAsyncQuery({
        query: queryString,
        context: {
          talaria: true,
        },
        cancelToken,
      });

      if (res instanceof IntermediateQueryState) return res;

      if (res.result) {
        return queryResultToAnalysis(analyzeQuery, res.result);
      } else {
        throw new Error(res.getErrorMessage() || 'unexpected destination');
      }
    },
    backgroundStatusCheck: async (
      execution: QueryExecution,
      analyzeQuery: AnalyzeQuery,
      cancelToken: CancelToken,
    ) => {
      const res = await talariaBackgroundStatusCheck(execution, analyzeQuery, cancelToken);
      if (res instanceof IntermediateQueryState) return res;

      if (res.result) {
        return queryResultToAnalysis(analyzeQuery, res.result);
      } else {
        throw new Error(res.getErrorMessage() || 'unexpected destination');
      }
    },
  });

  const analysis = analyzeQueryState.data;

  useEffect(() => {
    if (!analysis) return;
    analyzeQueryManager.reset();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryResult]);

  function renderOverallResult() {
    if (!analysis) return;
    const rollup = analysis.overall / analysis.count;
    return (
      <>
        <p>
          {`Estimated size after rollup is `}
          <span className="strong">{formatPercentClapped(rollup)}</span>
          {` of raw data size.`}
        </p>
        <p>{rollup < 0.8 ? 'That is decent.' : 'At this rate rollup is not worth it.'}</p>
      </>
    );
  }

  function renderDimension(dimensionIndex: number) {
    if (!queryResult) return;
    const outputName = dimensions[dimensionIndex].getOutputName();
    const headerIndex = queryResult.header.findIndex(c => c.name === outputName);
    if (headerIndex === -1) return;
    const column = queryResult.header[headerIndex];

    return (
      <Popover2
        content={
          <Menu>
            <ColumnActionMenu
              column={column}
              headerIndex={headerIndex}
              queryResult={queryResult}
              grouped
              onEditColumn={onEditColumn}
              onQueryAction={onQueryAction}
            />
          </Menu>
        }
      >
        <Tag interactive intent={Intent.WARNING}>
          {outputName}
        </Tag>
      </Popover2>
    );
  }

  let singleDimensionSuggestions: JSX.Element[] = [];
  let pairDimensionSuggestions: JSX.Element[] = [];
  let implicationSuggestions: JSX.Element[] = [];
  if (analysis) {
    const { count, counts, pairCounts, implications } = analysis;

    singleDimensionSuggestions = filterMap(counts, (c, i) => {
      const variability = c / count;
      if (variability < 0.8) return;
      return (
        <p key={`single_${i}`}>
          {renderDimension(i)}
          {` has variability of ${formatPercentClapped(variability)}`}
        </p>
      );
    });

    pairDimensionSuggestions = filterMap(pairs(analysis.counts.length), ([i, j], index) => {
      if (typeof pairCounts[index] !== 'number') return;
      const variability = pairCounts[index] / count;
      if (variability < 0.8) return;
      return (
        <p key={`pair_${index}`}>
          {renderDimension(i)}
          {' together with '}
          {renderDimension(j)}
          {` has variability of ${formatPercentClapped(variability)}`}
        </p>
      );
    });

    implicationSuggestions = filterMap(dimensions, (_d, i) => {
      if (implications.some(imp => imp.type === 'imply' && imp.b === i)) return;
      return (
        <p key={`imply_${i}`}>
          {'Remove '}
          {renderDimension(i)}
          {` (variability of ${formatPercentClapped(counts[i] / count)})`}
        </p>
      );
    });
  }

  return (
    <Callout className="rollup-analysis-pane" title="Analyze rollup">
      {analyzeQueryState.isInit() && (
        <>
          <p>
            Use this tool to analyze which dimensions are preventing the data from rolling up
            effectively.
          </p>
          <p>
            <Button
              text="Run analysis"
              onClick={() => {
                analyzeQueryManager.runQuery({ expressions: dimensions, deep: false });
              }}
            />
          </p>
        </>
      )}
      {analyzeQueryState.isLoading() && <p>Loading analysis...</p>}
      {renderOverallResult()}
      {singleDimensionSuggestions.length === 0 && analysis && !analysis.deep && (
        <p>
          <Button
            text="Deep analysis"
            onClick={() => {
              analyzeQueryManager.runQuery({ expressions: dimensions, deep: true });
            }}
          />
        </p>
      )}
      {(singleDimensionSuggestions.length || pairDimensionSuggestions.length > 0) && (
        <>
          <p>Poor rollup is caused by:</p>
          {singleDimensionSuggestions}
          {pairDimensionSuggestions}
        </>
      )}
      {analysis?.deep &&
        !singleDimensionSuggestions.length &&
        !pairDimensionSuggestions.length &&
        implicationSuggestions.length > 0 && (
          <>
            <p>Possible actions to improve rollup:</p>
            {implicationSuggestions}
          </>
        )}
      {analyzeQueryState.isError() && <p>{analyzeQueryState.getErrorMessage()}</p>}
      <p>
        <Button text="Close" onClick={onClose} />
      </p>
    </Callout>
  );
});
