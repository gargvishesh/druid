/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

public class SumOverTimeseriesExprMacro extends UnaryTimeseriesExprMacro
{
  public static final String NAME = "sum_over_timeseries";

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
    double result = 0;
    for (double dataPoint : simpleTimeSeriesContainer.computeSimple().getDataPoints()) {
      result += dataPoint;
    }
    return ExprEval.ofDouble(result);
  }
}
