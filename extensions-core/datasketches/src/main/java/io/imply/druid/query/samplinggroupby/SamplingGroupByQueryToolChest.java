/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.query.samplinggroupby.metrics.SamplingGroupByQueryMetricsFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResultMergeQueryRunner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.segment.column.RowSignature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For merging segment results at historical and broker, we use a sorted merge algorithm where the results for same
 * groups are merged and then only groups with lowest hashes amongst the segments/historicals are chosen
 * to avoid partial results at a group level.
 * The implementation doesn’t reuse any of the existing groupby merge code as of now.
 * While merging, the toolchest achieves the following things :
 * 1. Recieve a set of sorted results from historicals (or from segments within a historical). The sorting is done on
 * the time, hash values of the dimensions and then on the actual values of the dimensions via
 * {@link SamplingGroupByQuery#generateIntermediateGroupByQuery()}
 * 2. Merge the sorted rows received where the hashes and dimension values are same.
 * 3. Pass the de-duplicated result sequence to {@link SamplingGroupByMergingSequence} for picking lowest hash groups
 * per grain.
 */
public class SamplingGroupByQueryToolChest extends QueryToolChest<ResultRow, SamplingGroupByQuery>
{
  private final SamplingGroupByQueryMetricsFactory queryMetricsFactory;

  @Inject
  public SamplingGroupByQueryToolChest(SamplingGroupByQueryMetricsFactory queryMetricsFactory)
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryMetrics<? super SamplingGroupByQuery> makeMetrics(SamplingGroupByQuery query)
  {
    return queryMetricsFactory.makeMetrics();
  }

  @Override
  public Function<ResultRow, ResultRow> makePreComputeManipulatorFn(
      SamplingGroupByQuery query,
      MetricManipulationFn fn
  )
  {
    if (MetricManipulatorFns.identity().equals(fn)) {
      return Functions.identity();
    }

    List<AggregatorFactory> aggregatorSpecs = ImmutableList.copyOf(query.getAggregatorSpecs());
    int aggregatorStart = query.getContextBoolean(GroupByStrategyV2.CTX_KEY_OUTERMOST, true) ?
                          query.getResultRowAggregatorStart() : query.getIntermediateResultRowAggregatorStart();
    return row -> {
      for (int i = 0; i < aggregatorSpecs.size(); i++) {
        AggregatorFactory agg = aggregatorSpecs.get(i);
        row.set(aggregatorStart + i, fn.manipulate(agg, row.get(aggregatorStart + i)));
      }

      return row;
    };
  }

  @Override
  public TypeReference<ResultRow> getResultTypeReference()
  {
    return new TypeReference<ResultRow>(){};
  }

  @Override
  public QueryRunner<ResultRow> mergeResults(QueryRunner<ResultRow> runner)
  {
    return (queryPlus, responseContext) -> {
      SamplingGroupByQuery samplingGroupByQuery = (SamplingGroupByQuery) queryPlus.getQuery();

      // Set up downstream context.
      ImmutableMap.Builder<String, Object> context = ImmutableMap.builder();
      context.put(QueryContexts.FINALIZE_KEY, false);
      context.put(GroupByStrategyV2.CTX_KEY_OUTERMOST, false);
      context.put(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true);

      // remove having spec and new override context since this will run on historicals as well
      QueryPlus<ResultRow> newQueryPlus =
          queryPlus.withQuery(
              samplingGroupByQuery.generateQueryWithoutHavingSpec().withOverriddenContext(context.build())
          );

      // merge exactly same groups here for a start, de-deuplicates the list of groups
      QueryRunner<ResultRow> deDupQueryRunner = new ResultMergeQueryRunner<>(
          runner,
          query -> ((SamplingGroupByQuery) query).generateIntermediateGroupByQuery().getResultOrdering(),
          query -> new IntermediateResultRowMergeFn((SamplingGroupByQuery) query)
      );
      Sequence<ResultRow> mergedSequence = new SamplingGroupByMergingSequence(
          newQueryPlus,
          deDupQueryRunner.run(newQueryPlus, responseContext)
      );

      // don't apply post-aggregators if the merge is not outermost
      if (!queryPlus.getQuery().getContextBoolean(GroupByStrategyV2.CTX_KEY_OUTERMOST, true)) {
        return mergedSequence;
      }

      mergedSequence = mergedSequence.map(
          row -> {
            // Adapted from GroupByStrategyV2
            // This function's purpose is to apply PostAggregators.
            SamplingGroupByQuery query = (SamplingGroupByQuery) queryPlus.getQuery();

            ResultRow rowWithPostAggregations = ResultRow.create(query.getResultRowSizeWithPostAggregators());

            // Copy everything that comes before the postaggregations.
            for (int i = 0, idxCounter = 0; i < query.getIntermediateResultRowPostAggregatorStart(); i++) {
              if (i == query.getIntermediateResultRowHashColumnIndex()) {
                continue; // don't copy the hash to the final result row
              }
              rowWithPostAggregations.set(idxCounter++, row.get(i));
            }

            // set sampling rate
            double samplingRate = row.getLong(query.getIntermediateResultRowThetaColumnIndex()) * 1D / Long.MAX_VALUE;
            rowWithPostAggregations.set(query.getResultRowSamplingRateColumnIndex(), samplingRate);

            // Compute postaggregations. We need to do this with a result-row map because PostAggregator.compute
            // expects a map. Some further design adjustment may eliminate the need for it, and speed up this function.
            Map<String, Object> mapForPostAggregationComputation = new HashMap<>();
            RowSignature signature = query.getResultRowSignature();

            for (int i = query.getResultRowDimensionStart(); i < query.getResultRowPostAggregatorStart(); i++) {
              String columnName = signature.getColumnName(i);
              mapForPostAggregationComputation.put(columnName, rowWithPostAggregations.getArray()[i]);
            }

            for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
              PostAggregator postAggregator = query.getPostAggregatorSpecs().get(i);
              Object value = postAggregator.compute(mapForPostAggregationComputation);

              rowWithPostAggregations.set(query.getResultRowPostAggregatorStart() + i, value);
              mapForPostAggregationComputation.put(postAggregator.getName(), value);
            }

            return rowWithPostAggregations;
          }
      );
      // apply the having-spec
      return samplingGroupByQuery.makePostProcessingFn().apply(mergedSequence);
    };
  }

  @Override
  public RowSignature resultArraySignature(SamplingGroupByQuery query)
  {
    return query.getResultRowSignature();
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(final SamplingGroupByQuery query, final Sequence<ResultRow> resultSequence)
  {
    return resultSequence.map(ResultRow::getArray);
  }
}
