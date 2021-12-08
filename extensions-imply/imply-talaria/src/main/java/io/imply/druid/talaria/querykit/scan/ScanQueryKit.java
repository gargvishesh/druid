/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.scan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.kernel.MaxCountShuffleSpec;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.DataSegmentTimelineView;
import io.imply.druid.talaria.querykit.DataSourcePlan;
import io.imply.druid.talaria.querykit.QueryKit;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.common.LimitFrameProcessorFactory;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Query;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidQuery;

import java.util.ArrayList;
import java.util.List;

public class ScanQueryKit implements QueryKit<ScanQuery>
{
  private final ObjectMapper jsonMapper;

  public ScanQueryKit(final ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  public static RowSignature getSignature(final ScanQuery scanQuery, final ObjectMapper jsonMapper)
  {
    try {
      final String s = scanQuery.getContextValue(DruidQuery.CTX_TALARIA_SCAN_SIGNATURE);
      return jsonMapper.readValue(s, RowSignature.class);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      final String queryId,
      final ScanQuery originalQuery,
      final DataSegmentTimelineView timelineView,
      final QueryKit<Query<?>> queryKit,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    Preconditions.checkState(originalQuery.getScanRowsOffset() == 0, "Must not have offset");

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

    final ScanQuery queryToRun = originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final RowSignature scanSignature = getSignature(queryToRun, jsonMapper);
    final ShuffleSpec shuffleSpec;
    final RowSignature signatureToUse;

    if (queryToRun.getOrderBys().isEmpty() && queryToRun.isLimited()) {
      // No ordering, but there is a limit. Limiting, for now, works by funneling everything through a single worker.
      // So there is no point in forcing any particular partitioning.
      // TODO(gianm): Write some javadoc that explains why it's OK to ignore the resultShuffleSpecFactory here
      shuffleSpec = new MaxCountShuffleSpec(ClusterBy.none(), 1, false);
      signatureToUse = scanSignature;
    } else {
      final RowSignature.Builder signatureBuilder = RowSignature.builder().addAll(scanSignature);
      final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(queryToRun.getContext());
      final List<ClusterByColumn> clusterByColumns = new ArrayList<>();

      // Add regular orderBys.
      for (final ScanQuery.OrderBy orderBy : queryToRun.getOrderBys()) {
        clusterByColumns.add(
            new ClusterByColumn(
                orderBy.getColumnName(),
                orderBy.getOrder() == ScanQuery.Order.DESCENDING
            )
        );
      }

      // Add partition boosting column.
      clusterByColumns.add(new ClusterByColumn(QueryKitUtils.PARTITION_BOOST_COLUMN, false));
      signatureBuilder.add(QueryKitUtils.PARTITION_BOOST_COLUMN, ColumnType.LONG);

      shuffleSpec = resultShuffleSpecFactory.build(
          QueryKitUtils.clusterByWithSegmentGranularity(
              new ClusterBy(clusterByColumns, 0),
              segmentGranularity
          ),
          false
      );

      signatureToUse = QueryKitUtils.signatureWithSegmentGranularity(
          signatureBuilder.build(),
          segmentGranularity
      );
    }

    // TODO(gianm): For shuffle, put non-participating columns in a complex container
    queryDefBuilder.add(
        StageDefinition.builder(Math.max(minStageNumber, queryDefBuilder.getNextStageNumber()))
                       .inputStages(dataSourcePlan.getInputStageNumbers())
                       .broadcastInputStages(dataSourcePlan.getBroadcastInputStageNumbers())
                       .shuffleSpec(shuffleSpec)
                       .signature(signatureToUse)
                       .maxWorkerCount(dataSourcePlan.isSingleWorker() ? 1 : maxWorkerCount)
                       .processorFactory(
                           new ScanQueryFrameProcessorFactory(queryToRun, dataSourcePlan.getBaseInputSpecs())
                       )
    );

    if (queryToRun.isLimited()) {
      final long limitPlusOffset = queryToRun.getScanRowsOffset() + queryToRun.getScanRowsLimit();

      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 1)
                         .inputStages(firstStageNumber)
                         .signature(signatureToUse)
                         .maxWorkerCount(1)
                         .shuffleSpec(new MaxCountShuffleSpec(ClusterBy.none(), 1, false))
                         .processorFactory(new LimitFrameProcessorFactory(limitPlusOffset))
      );
    }

    return queryDefBuilder.build();
  }
}
