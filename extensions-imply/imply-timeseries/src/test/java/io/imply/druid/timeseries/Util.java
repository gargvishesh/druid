/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.expression.ArithmeticOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.DeltaTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.InterpolationTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.MaxOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.TimeWeightedAverageTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.TimeseriesToJSONExprMacro;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.math.expr.ExprMacroTable;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;
import static io.imply.druid.timeseries.SimpleTimeSeriesFromByteBufferAdapaterTest.VISIBLE_WINDOW;

public class Util
{
  public static SimpleTimeSeries makeSimpleTS(long[] ts, double[] dp)
  {
    return new SimpleTimeSeries(new ImplyLongArrayList(ts),
                                new ImplyDoubleArrayList(dp),
                                VISIBLE_WINDOW,
                                MAX_ENTRIES);
  }

  public static ExprMacroTable makeTimeSeriesMacroTable()
  {
    ImmutableList.Builder<ExprMacroTable.ExprMacro> macros =
        ImmutableList.<ExprMacroTable.ExprMacro>builder()
                     .add(
                         new MaxOverTimeseriesExprMacro(),
                         new TimeseriesToJSONExprMacro(),
                         new TimeWeightedAverageTimeseriesExprMacro(),
                         new DeltaTimeseriesExprMacro()
                     )
                     .addAll(InterpolationTimeseriesExprMacro.getMacros())
                     .addAll(ArithmeticOverTimeseriesExprMacro.getMacros());
    return new ExprMacroTable(macros.build());
  }
}
