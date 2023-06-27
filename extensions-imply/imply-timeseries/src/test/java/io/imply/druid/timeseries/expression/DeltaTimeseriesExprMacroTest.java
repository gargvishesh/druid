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
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DeltaTimeseriesExprMacroTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      Collections.singletonList(new DeltaTimeseriesExprMacro())
  );

  @Test
  public void testSimpleDeltaTimeseries()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.001s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                Intervals.ETERNITY,
                100
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3}),
        new ImplyDoubleArrayList(new double[]{1, 1, 1}),
        Intervals.ETERNITY,
        100
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithBucketMillis()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.010s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                Intervals.ETERNITY,
                100
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0}),
        new ImplyDoubleArrayList(new double[]{3}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        10L
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithBucketMillis2()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.010s')", MACRO_TABLE);

    Assert.assertThrows(IAE.class, () -> expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                Intervals.utc(1, 5),
                100
            )
        )
    ));
  }

  @Test
  public void testSimpleDeltaTimeseries_WithEndPoint()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.001s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                Intervals.utc(1, 5),
                null,
                new TimeSeries.EdgePoint(5, 5),
                100,
                null
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 1, 1, 1}),
        Intervals.utc(1, 5),
        null,
        null,
        100,
        1L
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithUnsortedValues()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.001s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                new ImplyDoubleArrayList(new double[]{1, 2, 4, 3}),
                Intervals.ETERNITY,
                100
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2}),
        new ImplyDoubleArrayList(new double[]{1, 2}),
        Intervals.ETERNITY,
        100
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithLongApartDataPoints()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.005s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 33, 44}),
                new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                Intervals.ETERNITY,
                100
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0, 30}),
        new ImplyDoubleArrayList(new double[]{2, 1}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        5L
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithLongApartDataPointsAndUnsorted()
  {
    final Expr expr = Parser.parse("delta_timeseries(ts, 'PT0.005s')", MACRO_TABLE);

    final ExprEval<?> result = expr.eval(
        Util.makeBinding(
            "ts",
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{1, 2, 33, 44}),
                new ImplyDoubleArrayList(new double[]{1, 2, 1.5, 3}),
                Intervals.ETERNITY,
                100
            )
        )
    );

    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0}),
        new ImplyDoubleArrayList(new double[]{2.5}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        5L
    );
    Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
  }
}
