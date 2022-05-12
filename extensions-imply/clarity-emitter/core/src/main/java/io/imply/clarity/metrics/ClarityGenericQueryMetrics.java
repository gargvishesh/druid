/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;

import java.util.concurrent.TimeUnit;

public class ClarityGenericQueryMetrics<QueryType extends Query<?>> extends DefaultQueryMetrics<QueryType>
{
  private final BaseClarityEmitterConfig config;

  public ClarityGenericQueryMetrics(BaseClarityEmitterConfig config)
  {
    super();
    this.config = config;
  }

  @Override
  public void query(QueryType query)
  {
    super.query(query);
    context(query);
  }

  @Override
  public void identity(String identity)
  {
    setDimension("identity", identity);
  }

  @Override
  public void interval(Query query)
  {
    // Don't emit.
  }

  @Override
  public void segment(final String segmentIdentifier)
  {
    if (config.isEmitSegmentDimension()) {
      setDimension("segment", segmentIdentifier);
    }
  }

  @Override
  public void sqlQueryId(QueryType query)
  {
    if (query.getSqlQueryId() != null) {
      setDimension("sqlQueryId", query.getSqlQueryId());
    }
  }

  @Override
  public void subQueryId(QueryType query)
  {
    if (query.getSubQueryId() != null) {
      setDimension("subQueryId", query.getSubQueryId());
    }
  }

  @Override
  public void context(final QueryType query)
  {
    // Override superclass to avoid storing JSON-ified "context". Instead, pull out the specific dimensions we want.
    ClarityMetricsUtils.addContextDimensions(config, this::setDimension, builder::getDimension, query.getContext());
  }

  @Override
  public void parallelMergeParallelism(int parallelism)
  {
    setDimension("mergeParallelism", parallelism);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeParallelism(int parallelism)
  {
    return reportMetric("query/merge/parallelism", parallelism);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputSequences(long numSequences)
  {
    return reportMetric("query/merge/sequences", numSequences);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeInputRows(long numRows)
  {
    return reportMetric("query/merge/rows/input", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeOutputRows(long numRows)
  {
    return reportMetric("query/merge/rows/output", numRows);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTaskCount(long numTasks)
  {
    return reportMetric("query/merge/tasks/count", numTasks);
  }

  @Override
  public QueryMetrics<QueryType> reportParallelMergeTotalCpuTime(long timeNs)
  {
    return reportMetric("query/merge/cpu", TimeUnit.NANOSECONDS.toMillis(timeNs));
  }
}
