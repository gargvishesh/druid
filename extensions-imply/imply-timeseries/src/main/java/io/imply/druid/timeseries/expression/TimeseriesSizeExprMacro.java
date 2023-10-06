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
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

@EverythingIsNonnullByDefault
public class TimeseriesSizeExprMacro extends UnaryTimeseriesExprMacro
{
  public static final String NAME = "timeseries_size";

  @Override
  public ExpressionType getType()
  {
    return ExpressionType.LONG;
  }

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public ExprEval compute(SimpleTimeSeriesContainer simpleTimeSeriesContainer)
  {
    if (simpleTimeSeriesContainer.isNull()) {
      return ExprEval.ofLong(null);
    }
    return ExprEval.ofLong(simpleTimeSeriesContainer.size());
  }
}
