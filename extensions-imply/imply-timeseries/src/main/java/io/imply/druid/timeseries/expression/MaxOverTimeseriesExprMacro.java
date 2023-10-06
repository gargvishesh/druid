/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

@EverythingIsNonnullByDefault
public class MaxOverTimeseriesExprMacro extends UnaryTimeseriesExprMacro
{
  public static final String NAME = "max_over_timeseries";

  @Override
  public ExpressionType getType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public ExprEval compute(SimpleTimeSeriesContainer simpleTimeSeriesContainer)
  {
    SimpleTimeSeries simpleTimeSeries = simpleTimeSeriesContainer.computeSimple();
    if (simpleTimeSeries.size() == 0) {
      return ExprEval.ofDouble(null);
    }
    double maxValue = simpleTimeSeries.getDataPoints().getDouble(0);
    for (double candidate : simpleTimeSeries.getDataPoints()) {
      maxValue = Math.max(maxValue, candidate);
    }
    return ExprEval.ofDouble(maxValue);
  }
}
