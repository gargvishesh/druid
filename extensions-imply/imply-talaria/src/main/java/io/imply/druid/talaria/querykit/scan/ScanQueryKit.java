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
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.input.StageInputSpec;
import io.imply.druid.talaria.kernel.MaxCountShuffleSpec;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.querykit.DataSourcePlan;
import io.imply.druid.talaria.querykit.QueryKit;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.common.OffsetLimitFrameProcessorFactory;
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
      final String s = scanQuery.getContextValue(DruidQuery.CTX_MULTI_STAGE_QUERY_SCAN_SIGNATURE);
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
      final QueryKit<Query<?>> queryKit,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
    final DataSourcePlan dataSourcePlan = DataSourcePlan.forDataSource(
        queryKit,
        queryId,
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        originalQuery.getFilter(),
        maxWorkerCount,
        minStageNumber,
        false
    );

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final ScanQuery queryToRun = originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final RowSignature scanSignature = getSignature(queryToRun, jsonMapper);
    final ShuffleSpec shuffleSpec;
    final RowSignature signatureToUse;
    final boolean hasLimitOrOffset = queryToRun.isLimited() || queryToRun.getScanRowsOffset() > 0;

    if (queryToRun.getOrderBys().isEmpty() && hasLimitOrOffset) {
      // No ordering, but there is a limit or an offset. These work by funneling everything through a single partition.
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

      final ClusterBy clusterBy =
          QueryKitUtils.clusterByWithSegmentGranularity(new ClusterBy(clusterByColumns, 0), segmentGranularity);
      shuffleSpec = resultShuffleSpecFactory.build(clusterBy, false);
      signatureToUse = QueryKitUtils.sortableSignature(
          QueryKitUtils.signatureWithSegmentGranularity(signatureBuilder.build(), segmentGranularity),
          clusterBy.getColumns()
      );
    }

    queryDefBuilder.add(
        StageDefinition.builder(Math.max(minStageNumber, queryDefBuilder.getNextStageNumber()))
                       .inputs(dataSourcePlan.getInputSpecs())
                       .broadcastInputs(dataSourcePlan.getBroadcastInputs())
                       .shuffleSpec(shuffleSpec)
                       .signature(signatureToUse)
                       .maxWorkerCount(dataSourcePlan.isSingleWorker() ? 1 : maxWorkerCount)
                       .processorFactory(new ScanQueryFrameProcessorFactory(queryToRun))
    );

    if (hasLimitOrOffset) {
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 1)
                         .inputs(new StageInputSpec(firstStageNumber))
                         .signature(signatureToUse)
                         .maxWorkerCount(1)
                         .shuffleSpec(new MaxCountShuffleSpec(ClusterBy.none(), 1, false))
                         .processorFactory(
                             new OffsetLimitFrameProcessorFactory(
                                 queryToRun.getScanRowsOffset(),
                                 queryToRun.isLimited() ? queryToRun.getScanRowsLimit() : null
                             )
                         )
      );
    }

    return queryDefBuilder.build();
  }
}
