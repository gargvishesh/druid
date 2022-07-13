/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import io.imply.druid.query.samplinggroupby.metrics.SamplingGroupByQueryMetrics;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class SamplingGroupByOnePassQueryEngine
{
  public static Sequence<ResultRow> process(
      SamplingGroupByQuery query,
      StorageAdapter storageAdapter,
      DruidProcessingConfig processingConfig,
      ResourceHolder<ByteBuffer> bufferHolder,
      Filter filter,
      Interval interval,
      @Nullable SamplingGroupByQueryMetrics samplingGroupByQueryMetrics
  )
  {
    try {
      GroupByQuery groupByQuery = query.generateIntermediateGroupByQuery();
      // create constant VCs for hash and theta and add that in the cursor
      VirtualColumn hashVirtualColumn = new ExpressionVirtualColumn(
          SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
          ExprEval.ofLong(0).toExpr(),
          ColumnType.LONG
      );
      VirtualColumn thetaVirtualColumn = new ExpressionVirtualColumn(
          SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
          ExprEval.ofLong(0).toExpr(),
          ColumnType.LONG
      );
      ImmutableList.Builder<VirtualColumn> virtualColumns = ImmutableList.builder();
      virtualColumns.add(hashVirtualColumn, thetaVirtualColumn);
      virtualColumns.addAll(Arrays.asList(groupByQuery.getVirtualColumns().getVirtualColumns()));
      Sequence<Cursor> cursors = storageAdapter.makeCursors(
          filter,
          interval,
          VirtualColumns.create(virtualColumns.build()),
          groupByQuery.getGranularity(),
          false,
          samplingGroupByQueryMetrics
      );
      Sequence<ResultRow> resultRowSequence =
          cursors.flatMap(
              cursor -> new BaseSequence<>(
                  new BaseSequence.IteratorMaker<ResultRow, GroupByQueryEngineV2.GroupByEngineIterator<ByteBuffer>>()
                  {
                    @Override
                    public GroupByQueryEngineV2.GroupByEngineIterator<ByteBuffer> make()
                    {
                      ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

                      ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
                          .createColumnSelectorPluses(
                              GroupByQueryEngineV2.STRATEGY_FACTORY,
                              groupByQuery.getDimensions(),
                              columnSelectorFactory
                          );
                      GroupByColumnSelectorPlus[] dims = GroupByQueryEngineV2.createGroupBySelectorPlus(
                          selectorPlus,
                          groupByQuery.getResultRowDimensionStart()
                      );

                      return new SamplingHashAggregateIterator(
                          query,
                          new GroupByQueryConfig(),
                          processingConfig,
                          cursor,
                          bufferHolder.get(),
                          dims,
                          true, // TODO : multi-value handling
                          query.getMaxGroups()
                      );
                    }

                    @Override
                    public void cleanup(GroupByQueryEngineV2.GroupByEngineIterator<ByteBuffer> iterFromMake)
                    {
                      iterFromMake.close();
                    }
                  }
              )
          ).withBaggage(bufferHolder);
      // sort the groups by time, groupHash, grouping dimensions
      List<ResultRow> resultGroups = resultRowSequence.toList();
      resultGroups.sort(query.generateIntermediateGroupByQuery().getResultOrdering());
      return Sequences.simple(resultGroups);
    }
    finally {
      bufferHolder.close();
    }
  }
}
