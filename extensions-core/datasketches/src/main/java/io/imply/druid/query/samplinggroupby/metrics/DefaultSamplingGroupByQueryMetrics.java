/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.metrics;

import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;

public class DefaultSamplingGroupByQueryMetrics extends DefaultQueryMetrics<SamplingGroupByQuery>
    implements SamplingGroupByQueryMetrics
{
  @Override
  public void query(SamplingGroupByQuery query)
  {
    super.query(query);
    numDimensions(query);
    numMetrics(query);
    numComplexMetrics(query);
    granularity(query);
  }

  @Override
  public void numDimensions(SamplingGroupByQuery query)
  {
    setDimension("numDimensions", String.valueOf(query.getDimensions().size()));
  }

  @Override
  public void numMetrics(SamplingGroupByQuery query)
  {
    setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
  }

  @Override
  public void numComplexMetrics(SamplingGroupByQuery query)
  {
    int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
    setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
  }

  @Override
  public void granularity(SamplingGroupByQuery query)
  {
    //Don't emit by default
  }
}
