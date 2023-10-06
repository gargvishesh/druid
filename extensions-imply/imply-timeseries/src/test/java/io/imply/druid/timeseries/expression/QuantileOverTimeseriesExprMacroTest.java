/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

public class QuantileOverTimeseriesExprMacroTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(new QuantileOverTimeseriesExprMacro())
  );

  @Test
  public void testQuantile()
  {
    double[] dataPoints = new double[]{1, 3, 2, 4};
    SimpleTimeSeries input = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(dataPoints),
        Intervals.ETERNITY,
        100
    );

    Expr expr = Parser.parse("quantile_over_timeseries(ts, 0.50)", MACRO_TABLE);
    ExprEval<?> result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(2, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0.51)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(3, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0.75)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(3, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0.95)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(4, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 1.00)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(4, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0.25)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(1, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0.26)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(2, result.asDouble(), 1e-6);

    expr = Parser.parse("quantile_over_timeseries(ts, 0)", MACRO_TABLE);
    result = expr.eval(Util.makeBinding("ts", input));
    Assert.assertEquals(1, result.asDouble(), 1e-6);

    Assert.assertThrows(Exception.class, () -> Parser.parse("quantile_over_timeseries(ts, 1.1)", MACRO_TABLE));

    Assert.assertArrayEquals(new double[]{1, 3, 2, 4}, input.getDataPoints().getDoubleArray(), 1e-6);
  }
}
