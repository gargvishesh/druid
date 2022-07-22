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
import org.apache.druid.query.topn.DefaultTopNQueryMetrics;
import org.apache.druid.query.topn.TopNQuery;

public class ClarityTopNQueryMetrics extends DefaultTopNQueryMetrics
{
  private final BaseClarityEmitterConfig config;

  public ClarityTopNQueryMetrics(BaseClarityEmitterConfig config)
  {
    this.config = config;
  }

  @Override
  public void query(TopNQuery query)
  {
    super.query(query);
    context(query);
  }

  @Override
  public void identity(final String identity)
  {
    setDimension("identity", identity);
  }

  @Override
  public void interval(final TopNQuery topNQuery)
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
  public void sqlQueryId(TopNQuery query)
  {
    if (query.getSqlQueryId() != null) {
      setDimension("sqlQueryId", query.getSqlQueryId());
    }
  }

  @Override
  public void subQueryId(TopNQuery query)
  {
    if (query.getSubQueryId() != null) {
      setDimension("subQueryId", query.getSubQueryId());
    }
  }

  @Override
  public void context(final TopNQuery query)
  {
    // Override superclass to avoid storing JSON-ified "context". Instead, pull out the specific dimensions we want.
    ClarityMetricsUtils.addContextDimensions(config, this::setDimension, builder::getDimension, query.getQueryContext());
  }
}
