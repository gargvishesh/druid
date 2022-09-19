/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.List;


public class SamplingGroupByUtils
{
  public static boolean canVectorize(
      final SamplingGroupByQuery query,
      final StorageAdapter adapter,
      @Nullable final Filter filter
  )
  {
    final ColumnInspector inspector = query.getVirtualColumns().wrapInspector(adapter);

    return adapter.canVectorize(filter, query.getVirtualColumns(), false)
           && canVectorizeDimensions(inspector, query.getDimensions())
           && VirtualColumns.shouldVectorize(query, query.getVirtualColumns(), adapter);
  }

  public static boolean canVectorizeDimensions(
      final ColumnInspector inspector,
      final List<DimensionSpec> dimensions
  )
  {
    return dimensions
        .stream()
        .allMatch(
            dimension -> {
              if (!dimension.canVectorize()) {
                return false;
              }

              if (dimension.mustDecorate()) {
                // group by on multi value dimensions are not currently supported
                // DimensionSpecs that decorate may turn singly-valued columns into multi-valued selectors.
                // To be safe, we must return false here.
                return false;
              }

              // Now check column capabilities.
              final ColumnCapabilities columnCapabilities = inspector.getColumnCapabilities(dimension.getDimension());
              // null here currently means the column does not exist, nil columns can be vectorized
              if (columnCapabilities == null) {
                return true;
              }
              // must be single valued
              return columnCapabilities.hasMultipleValues().isFalse();
            });
  }

  /**
   * Generates an intermediate GroupBy query which is used in per segment query processing. The intermediate query
   * doesn't include the post aggregators present in the original query. It is ok to do so since post aggregators are
   * always processed in brokers using the original {@link SamplingGroupByQuery}.
   */
  public static GroupByQuery generateIntermediateGroupByQuery(SamplingGroupByQuery samplingGroupByQuery)
  {
    ImmutableList.Builder<DimensionSpec> dimensionsWithHashAndTheta = ImmutableList.builder();
    dimensionsWithHashAndTheta
        .add(
            new DefaultDimensionSpec(
                SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                ColumnType.LONG
            )
        )
        .addAll(samplingGroupByQuery.getDimensions())
        .add(
            new DefaultDimensionSpec(
                SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                ColumnType.LONG
            ));
    return GroupByQuery.builder()
                       .setDataSource(samplingGroupByQuery.getDataSource())
                       .setInterval(samplingGroupByQuery.getQuerySegmentSpec())
                       .setDimensions(dimensionsWithHashAndTheta.build())
                       .setVirtualColumns(samplingGroupByQuery.getVirtualColumns())
                       .setAggregatorSpecs(samplingGroupByQuery.getAggregatorSpecs())
                       .setDimFilter(samplingGroupByQuery.getDimFilter())
                       .setHavingSpec(samplingGroupByQuery.getHavingSpec())
                       .setGranularity(samplingGroupByQuery.getGranularity())
                       .setContext(samplingGroupByQuery.getContext())
                       .build();
  }
}
