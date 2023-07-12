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

public class IRROverTimeseriesMacroTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(new IRROverTimeseriesExprMacro(), new IRRDebugOverTimeseriesExprMacro())
  );

  @Test
  public void testIRR()
  {
    final Expr expr = Parser.parse("irr(ts, 4774.81365, 14996.387669999998, '2006-11-30T18:30:00.000Z/2010-12-30T18:30:00.000Z', 0.1)", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1164931200000L, 1165795200000L, 1166486400000L, 1166659200000L, 1175040000000L, 1203638400000L, 1293753600000L}),
                new ImplyDoubleArrayList(new double[]{0, 2, -32, 32, 4967, 5000, 0}),
                Intervals.ETERNITY,
                100
            )
        )
    );
    Assert.assertEquals(result.asDouble(), 0.019676479264537836D, 1e-6);
  }

  @Test
  public void testIRRDebug()
  {
    final Expr expr = Parser.parse("irr_debug(ts, 4774.81365, 14996.387669999998, '2006-11-30T18:30:00.000Z/2010-12-30T18:30:00.000Z', 0.1)", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1164931200000L, 1165795200000L, 1166486400000L, 1166659200000L, 1175040000000L, 1203638400000L, 1293753600000L}),
                new ImplyDoubleArrayList(new double[]{0, 2, -32, 32, 4967, 5000, 0}),
                Intervals.ETERNITY,
                100
            )
        )
    );
    Assert.assertEquals(
        "{cashFlows=SimpleTimeSeries{timestamps=[1164931200000, 1165795200000, 1166486400000, 1166659200000, "
        + "1175040000000, 1203638400000, 1293753600000], dataPoints=[0.0, 2.0, -32.0, 32.0, 4967.0, 5000.0, 0.0], "
        + "maxEntries=100, start=EdgePoint{timestamp=-1, data=-1.0}, end=EdgePoint{timestamp=-1, data=-1.0}, "
        + "bucketMillis=1}, startValue=4774.81365, endValue=14996.387669999998, startEstimate=0.1, "
        + "window=2006-11-30T18:30:00.000Z/2010-12-30T18:30:00.000Z, "
        + "iterations=[{iteration=1, npv=932.4170726765988, npvDerivative=10713.4105523886, estimate=0.012967297564386485}, "
        + "{iteration=2, npv=-84.9590975566025, npvDerivative=12751.566530403356, estimate=0.019629937590146133}, "
        + "{iteration=3, npv=-0.585301158227594, npvDerivative=12576.456378206465, estimate=0.019676477023880572}, "
        + "{iteration=4, npv=-2.817681524902582E-5, npvDerivative=12575.245527322819, estimate=0.019676479264537836}, "
        + "{iteration=5, npv=0.0, npvDerivative=12575.24546903006, estimate=0.019676479264537836}]}",
        result.asString()
    );
  }
}
