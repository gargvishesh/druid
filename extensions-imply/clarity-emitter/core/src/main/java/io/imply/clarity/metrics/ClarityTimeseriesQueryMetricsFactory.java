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
import org.apache.druid.query.timeseries.TimeseriesQueryMetrics;
import org.apache.druid.query.timeseries.TimeseriesQueryMetricsFactory;

public class ClarityTimeseriesQueryMetricsFactory implements TimeseriesQueryMetricsFactory
{
  private final BaseClarityEmitterConfig config;

  @Inject
  public ClarityTimeseriesQueryMetricsFactory(BaseClarityEmitterConfig config)
  {
    this.config = config;
  }

  @Override
  public TimeseriesQueryMetrics makeMetrics()
  {
    return new ClarityTimeseriesQueryMetrics(config);
  }
}
