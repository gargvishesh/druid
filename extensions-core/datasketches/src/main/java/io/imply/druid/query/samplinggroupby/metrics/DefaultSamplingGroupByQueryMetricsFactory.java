/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.metrics;

import com.google.common.annotations.VisibleForTesting;

public class DefaultSamplingGroupByQueryMetricsFactory implements SamplingGroupByQueryMetricsFactory
{
  private static final SamplingGroupByQueryMetricsFactory INSTANCE =
      new DefaultSamplingGroupByQueryMetricsFactory();

  @VisibleForTesting
  public static SamplingGroupByQueryMetricsFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public SamplingGroupByQueryMetrics makeMetrics()
  {
    return new DefaultSamplingGroupByQueryMetrics();
  }
}
