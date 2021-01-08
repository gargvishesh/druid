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
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;

public class ClarityGenericQueryMetricsFactory implements GenericQueryMetricsFactory
{
  private final BaseClarityEmitterConfig config;

  @Inject
  public ClarityGenericQueryMetricsFactory(BaseClarityEmitterConfig config)
  {
    this.config = config;
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(Query<?> query)
  {
    ClarityGenericQueryMetrics<Query<?>> queryMetrics = new ClarityGenericQueryMetrics<>(config);
    queryMetrics.query(query);
    return queryMetrics;
  }
}
