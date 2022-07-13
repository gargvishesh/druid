/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.imply.druid.query.samplinggroupby.engine.onepass.SamplingGroupByOnePassQueryEngine;
import io.imply.druid.query.samplinggroupby.engine.twopass.SamplingGroupByTwoPassQueryEngine;
import io.imply.druid.query.samplinggroupby.metrics.SamplingGroupByQueryMetrics;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.List;

public class SamplingGroupByQueryRunnerFactory implements QueryRunnerFactory<ResultRow, SamplingGroupByQuery>
{
  private final SamplingGroupByQueryToolChest queryToolChest;
  private final DruidProcessingConfig processingConfig;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final QueryWatcher queryWatcher;

  @Inject
  public SamplingGroupByQueryRunnerFactory(
      SamplingGroupByQueryToolChest samplingGroupByQueryToolChest,
      DruidProcessingConfig processingConfig,
      @Global NonBlockingPool<ByteBuffer> bufferPool,
      QueryWatcher queryWatcher
  )
  {
    this.queryToolChest = samplingGroupByQueryToolChest;
    this.processingConfig = processingConfig;
    this.bufferPool = bufferPool;
    this.queryWatcher = queryWatcher;
  }

  /**
   *  There are two algorithms that can be used to run the query :
   *
   * Vectorized : This algorithm is built upon the existing vectorized execution for groupBy queries. To sample the
   * groups for the resultset, a two-pass algorithm is implemented where :
   * 1. The first pass creates a uniform hash for all the rows in the segment based on the dimension columns being used.
   * Further, the hashes for each row are also added to a memory backed virtual column and a ‘HeapQuickSelectSketch’
   * which helps in determining the sampling rate over the input hashes given a sampling limit.
   * 2. After the completion of the first pass, the sketch provides a hash cutoff. We want to pick all rows in the segment
   * for aggregation which have their hashes less than the cutoff.
   * 3. The second pass is an invocation of the vectorized groupBy engine along with a filter over the hashing virtual
   * column to remove all the rows greater than or equal to the cutoff.
   *
   * Non-Vectorized : This algorithm is built upon the existing non-vectorized execution for groupBy queries. To sample
   * the groups for the resultset, a single-pass algorithm is implemented where :
   * 1. For aggregation, ‘HashAggregateIterator’ is used as in normal non-vectorized execution
   * 2. For each row, we compute a hash value for the grouping dimensions
   * 3. Upon every resize of the hashtable, the aggregation ‘Grouper’ filters away the groups which fall below the hash
   * cutoff from the sketch at that moment. This allows to proactively sample the aggregation groups instead of
   * accumulating them and then filtering at the end of the pass.
   * 4. The hash cutoff updates from the sketch are also percolated upto the row cursor which eliminates any further rows
   * with hashes beyond the cutoff.
   *
   * As can be seen, the vectorized and non-vectorized strategies implement different flavors of the sampling algorithms
   * as well. Both the approaches have some niceties and we haven’t evaluated them from a performance point of view yet.
   * We may converge to a single algorithm or augment the condition which dictates the algorithm in future.
   */
  @Override
  public QueryRunner<ResultRow> createRunner(Segment segment)
  {
    return (queryPlus, responseContext) -> {
      if (!(queryPlus.getQuery() instanceof SamplingGroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", queryPlus.getQuery().getClass(), SamplingGroupByQuery.class);
      }

      SamplingGroupByQuery query = (SamplingGroupByQuery) queryPlus.getQuery();
      StorageAdapter storageAdapter = segment.asStorageAdapter();

      List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
      if (intervals.size() != 1) {
        throw new IAE("Should only have one interval, got[%s]", intervals);
      }

      Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
      Interval interval = Iterables.getOnlyElement(query.getIntervals());

      boolean doVectorize = QueryContexts.getVectorize(query).shouldVectorize(
          SamplingGroupByUtils.canVectorize(query, storageAdapter, filter)
      );

      if (doVectorize) {
        return SamplingGroupByTwoPassQueryEngine.process(
            query,
            storageAdapter,
            processingConfig,
            bufferPool.take(),
            filter,
            interval,
            (SamplingGroupByQueryMetrics) queryPlus.getQueryMetrics()
        );
      } else {
        return SamplingGroupByOnePassQueryEngine.process(
            query,
            storageAdapter,
            processingConfig,
            bufferPool.take(),
            filter,
            interval,
            (SamplingGroupByQueryMetrics) queryPlus.getQueryMetrics()
        );
      }
    };
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return new SamplingGroupByMergingQueryRunner(queryProcessingPool, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<ResultRow, SamplingGroupByQuery> getToolchest()
  {
    return queryToolChest;
  }
}
