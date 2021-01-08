/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import com.google.inject.Inject;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import org.apache.druid.query.topn.TopNQueryMetrics;
import org.apache.druid.query.topn.TopNQueryMetricsFactory;

public class ClarityTopNQueryMetricsFactory implements TopNQueryMetricsFactory
{
  private final BaseClarityEmitterConfig config;

  @Inject
  public ClarityTopNQueryMetricsFactory(BaseClarityEmitterConfig config)
  {
    this.config = config;
  }

  @Override
  public TopNQueryMetrics makeMetrics()
  {
    return new ClarityTopNQueryMetrics(config);
  }
}
