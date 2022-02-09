/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.groupby;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.kernel.MaxCountShuffleSpec;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpecFactories;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.DataSegmentTimelineView;
import io.imply.druid.talaria.querykit.DataSourcePlan;
import io.imply.druid.talaria.querykit.QueryKit;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.common.OffsetLimitFrameProcessorFactory;
import io.imply.druid.talaria.querykit.common.OrderByFrameProcessorFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.AlwaysHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class GroupByQueryKit implements QueryKit<GroupByQuery>
{
  @Override
  public QueryDefinition makeQueryDefinition(
      final String queryId,
      final GroupByQuery originalQuery,
      final DataSegmentTimelineView timelineView,
      final QueryKit<Query<?>> queryKit,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    validateQuery(originalQuery);

    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
    final DataSourcePlan dataSourcePlan = QueryKitUtils.makeDataSourcePlan(
        queryKit,
        queryId,
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        timelineView,
        originalQuery.getFilter(),
        maxWorkerCount,
        minStageNumber
    );

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final GroupByQuery queryToRun = (GroupByQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());

    final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(queryToRun.getContext());
    final RowSignature signaturePreAggregation = computeSignaturePreAggregation(queryToRun);
    final RowSignature signaturePostAggregation = computeSignaturePostAggregation(queryToRun);
    final RowSignature signatureForResults =
        QueryKitUtils.signatureWithSegmentGranularity(signaturePostAggregation, segmentGranularity);
    final ClusterBy clusterByPreAggregation = computeClusterByPreAggregation(queryToRun);
    final ClusterBy clusterByForResults =
        QueryKitUtils.clusterByWithSegmentGranularity(computeClusterByForResults(queryToRun), segmentGranularity);
    final boolean doOrderBy = !clusterByForResults.equals(clusterByPreAggregation);
    final boolean doLimitOrOffset =
        queryToRun.getLimitSpec() instanceof DefaultLimitSpec
        && (((DefaultLimitSpec) queryToRun.getLimitSpec()).isLimited()
            || ((DefaultLimitSpec) queryToRun.getLimitSpec()).isOffset());

    final ShuffleSpecFactory shuffleSpecFactoryForAggregation;
    final ShuffleSpecFactory shuffleSpecFactoryForOrderBy;

    if (clusterByPreAggregation.getColumns().isEmpty()) {
      // Ignore shuffleSpecFactory, since we know only a single partition will come out, and we can save some effort.
      shuffleSpecFactoryForAggregation = ShuffleSpecFactories.singlePartition();
      shuffleSpecFactoryForOrderBy = ShuffleSpecFactories.singlePartition();
    } else if (doOrderBy) {
      shuffleSpecFactoryForAggregation = ShuffleSpecFactories.subQueryWithMaxWorkerCount(maxWorkerCount);
      shuffleSpecFactoryForOrderBy = doLimitOrOffset
                                     ? ShuffleSpecFactories.singlePartition()
                                     : resultShuffleSpecFactory;
    } else {
      shuffleSpecFactoryForAggregation = doLimitOrOffset
                                         ? ShuffleSpecFactories.singlePartition()
                                         : resultShuffleSpecFactory;
      shuffleSpecFactoryForOrderBy = null;
    }

    queryDefBuilder.add(
        StageDefinition.builder(firstStageNumber)
                       .inputStages(dataSourcePlan.getInputStageNumbers())
                       .broadcastInputStages(dataSourcePlan.getBroadcastInputStageNumbers())
                       .signature(signaturePreAggregation)
                       .shuffleSpec(shuffleSpecFactoryForAggregation.build(clusterByPreAggregation, true))
                       .maxWorkerCount(dataSourcePlan.isSingleWorker() ? 1 : maxWorkerCount)
                       .processorFactory(
                           new GroupByPreShuffleFrameProcessorFactory(
                               queryToRun,
                               dataSourcePlan.getBaseInputSpecs()
                           )
                       )
    );

    queryDefBuilder.add(
        StageDefinition.builder(firstStageNumber + 1)
                       .inputStages(firstStageNumber)
                       .signature(doOrderBy || doLimitOrOffset ? signaturePostAggregation : signatureForResults)
                       .maxWorkerCount(maxWorkerCount)
                       .processorFactory(new GroupByPostShuffleFrameProcessorFactory(queryToRun))
    );

    if (doOrderBy) {
      final VirtualColumns virtualColumns;
      final VirtualColumn segmentGranularityVirtualColumn = QueryKitUtils.makeSegmentGranularityVirtualColumn(
          segmentGranularity,
          queryToRun.getContextValue(QueryKitUtils.CTX_TIME_COLUMN_NAME)
      );

      if (segmentGranularityVirtualColumn == null) {
        virtualColumns = VirtualColumns.EMPTY;
      } else {
        virtualColumns = VirtualColumns.create(Collections.singletonList(segmentGranularityVirtualColumn));
      }

      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 2)
                         .inputStages(firstStageNumber + 1)
                         .signature(signatureForResults)
                         .shuffleSpec(shuffleSpecFactoryForOrderBy.build(clusterByForResults, false))
                         .maxWorkerCount(maxWorkerCount)
                         .processorFactory(new OrderByFrameProcessorFactory(virtualColumns))
      );
    }

    if (doLimitOrOffset) {
      final DefaultLimitSpec limitSpec = (DefaultLimitSpec) queryToRun.getLimitSpec();

      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + (doOrderBy ? 3 : 2))
                         .inputStages(firstStageNumber + (doOrderBy ? 2 : 1))
                         .signature(signatureForResults)
                         .maxWorkerCount(1)
                         .shuffleSpec(new MaxCountShuffleSpec(ClusterBy.none(), 1, false))
                         .processorFactory(
                             new OffsetLimitFrameProcessorFactory(
                                 limitSpec.getOffset(),
                                 limitSpec.isLimited() ? (long) limitSpec.getLimit() : null
                             )
                         )
      );
    }

    return queryDefBuilder.queryId(queryId).build();
  }

  static RowSignature computeSignaturePreAggregation(final GroupByQuery query)
  {
    final RowSignature postAggregationSignature = query.getResultRowSignature(RowSignature.Finalization.NO);
    final RowSignature.Builder builder = RowSignature.builder();

    for (int i = 0; i < query.getResultRowSizeWithoutPostAggregators(); i++) {
      builder.add(
          postAggregationSignature.getColumnName(i),
          postAggregationSignature.getColumnType(i).orElse(null)
      );
    }

    return builder.build();
  }

  static RowSignature computeSignaturePostAggregation(final GroupByQuery query)
  {
    final RowSignature.Finalization finalization =
        isFinalize(query) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO;
    return query.getResultRowSignature(finalization);
  }

  static boolean isFinalize(final GroupByQuery query)
  {
    // TODO(gianm): This is a discrepancy between native and talaria execution: native by default finalizes outer
    //   queries only; talaria by default finalizes all queries, including subqueries.
    return QueryContexts.isFinalize(query, true);
  }

  static ClusterBy computeClusterByPreAggregation(final GroupByQuery query)
  {
    final List<ClusterByColumn> columns = new ArrayList<>();

    for (final DimensionSpec dimension : query.getDimensions()) {
      columns.add(new ClusterByColumn(dimension.getOutputName(), false));
    }

    // Note: ignoring time because we assume granularity = all.
    return new ClusterBy(columns, 0);
  }

  static ClusterBy computeClusterByForResults(final GroupByQuery query)
  {
    if (query.getLimitSpec() instanceof DefaultLimitSpec) {
      final DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) query.getLimitSpec();

      if (!defaultLimitSpec.getColumns().isEmpty()) {
        final List<ClusterByColumn> clusterByColumns = new ArrayList<>();

        for (final OrderByColumnSpec orderBy : defaultLimitSpec.getColumns()) {
          clusterByColumns.add(
              new ClusterByColumn(
                  orderBy.getDimension(),
                  orderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING
              )
          );
        }

        return new ClusterBy(clusterByColumns, 0);
      }
    }

    return computeClusterByPreAggregation(query);
  }

  private static void validateQuery(final GroupByQuery query)
  {
    // Misc features that we do not support right now.
    Preconditions.checkState(!query.getContextSortByDimsFirst(), "Must not sort by dims first");
    Preconditions.checkState(query.getSubtotalsSpec() == null, "Must not have 'subtotalsSpec'");
    // Matches condition in GroupByPostShuffleWorker.makeHavingFilter.
    Preconditions.checkState(
        query.getHavingSpec() == null
        || query.getHavingSpec() instanceof DimFilterHavingSpec
        || query.getHavingSpec() instanceof AlwaysHavingSpec,
        "Must use 'filter' or 'always' havingSpec"
    );
    Preconditions.checkState(query.getGranularity().equals(Granularities.ALL), "Must have granularity 'all'");
    Preconditions.checkState(
        query.getLimitSpec() instanceof NoopLimitSpec || query.getLimitSpec() instanceof DefaultLimitSpec,
        "Must have noop or default limitSpec"
    );

    if (query.getLimitSpec() instanceof DefaultLimitSpec) {
      final DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) query.getLimitSpec();

      final RowSignature resultSignature = computeSignaturePostAggregation(query);
      for (final OrderByColumnSpec column : defaultLimitSpec.getColumns()) {
        final Optional<ColumnType> type = resultSignature.getColumnType(column.getDimension());

        if (!type.isPresent() || !isNaturalComparator(type.get().getType(), column.getDimensionComparator())) {
          throw new ISE(
              "Must use natural comparator for column [%s] of type [%s]",
              column.getDimension(),
              type.orElse(null)
          );
        }
      }
    }
  }

  private static boolean isNaturalComparator(final ValueType type, final StringComparator comparator)
  {
    return ((type == ValueType.STRING && StringComparators.LEXICOGRAPHIC.equals(comparator))
            || (type.isNumeric() && StringComparators.NUMERIC.equals(comparator)))
           && !type.isArray();
  }
}
