/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import io.imply.druid.query.aggregation.datasketches.tuple.SampledAvgScoreToHistogramPostAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.sql.DoublesSketchListArgBaseOperatorConversion;

public class SampledAvgScoreToHistogramOperatorConversion extends DoublesSketchListArgBaseOperatorConversion
{
  private static final String FUNCTION_NAME = "SAMPLED_AVG_SCORE_HISTOGRAM";

  @Override
  public String getFunctionName()
  {
    return FUNCTION_NAME;
  }

  @Override
  public PostAggregator makePostAgg(String name, PostAggregator field, double[] args)
  {
    return new SampledAvgScoreToHistogramPostAggregator(name, field, args);
  }
}
