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
import com.google.common.collect.ImmutableMap;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ArithmeticTimeseriesExprMacroTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.<ExprMacroTable.ExprMacro>builder().addAll(ArithmeticTimeseriesExprMacro.getMacros()).build()
  );

  @Test
  public void testSimple()
  {
    double[][] expectedDataPoints = new double[][] {
        new double[]{2, 4, 6, 8},
        new double[]{0, 0, 0, 0},
        new double[]{1, 4, 9, 16},
        new double[]{1, 1, 1, 1}
    };
    ArithmeticTimeseriesExprMacro[] macros = new ArithmeticTimeseriesExprMacro[] {
        new ArithmeticTimeseriesExprMacro.AddTimeseriesExprMacro(),
        new ArithmeticTimeseriesExprMacro.SubtractTimeseriesExprMacro(),
        new ArithmeticTimeseriesExprMacro.MultiplyTimeseriesExprMacro(),
        new ArithmeticTimeseriesExprMacro.DivideTimeseriesExprMacro()
    };

    for (int i = 0; i < macros.length; i++) {
      Expr expr = Parser.parse(
          StringUtils.format("%s(ts, ts1)", macros[i].name()),
          MACRO_TABLE
      );

      ExprEval<?> result = expr.eval(
          Util.makeBindings(
              ImmutableMap.of(
              "ts",
              SimpleTimeSeriesContainer.createFromInstance(
                  new SimpleTimeSeries(
                      new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                      new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                      Intervals.ETERNITY,
                      100
                  )
              ),
              "ts1",
              SimpleTimeSeriesContainer.createFromInstance(
                new SimpleTimeSeries(
                    new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                    new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                    Intervals.ETERNITY,
                    100
                )
              )
          ))
      );

      SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
          new ImplyDoubleArrayList(expectedDataPoints[i]),
          Intervals.ETERNITY,
          100
      );
      Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
    }
  }

  @Test
  public void testErrors()
  {
    String[] exprs = new String[]{
        "add_timeseries(ts, ts1)",
        "subtract_timeseries(ts, ts1)",
        "multiply_timeseries(ts, ts1)",
        "divide_timeseries(ts, ts1)"
    };
    for (String expr : exprs) {
      Expr parsed = Parser.parse(expr, MACRO_TABLE);

      // bad type of argument, result exception. catching general exception since there might be exception in deserializing
      // the input too
      Assert.assertThrows(
          Exception.class,
          () -> parsed.eval(
              Util.makeBindings(
                  ImmutableMap.of(
                      "ts",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.ETERNITY,
                              100
                          )
                      ),
                      "ts1",
                      "foo"
                  ))
          )
      );

      // non-identical windows, result ISE
      Assert.assertThrows(
          ISE.class,
          () -> parsed.eval(
              Util.makeBindings(
                  ImmutableMap.of(
                      "ts",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.ETERNITY,
                              100
                          )
                      ),
                      "ts1",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.utc(1, 5),
                              100
                          )
                      )
                  ))
          )
      );

      // non-identical timestamp sizes, result ISE
      Assert.assertThrows(
          ISE.class,
          () -> parsed.eval(
              Util.makeBindings(
                  ImmutableMap.of(
                      "ts",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.ETERNITY,
                              100
                          )
                      ),
                      "ts1",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3}),
                              Intervals.ETERNITY,
                              100
                          )
                      )
                  ))
          )
      );

      // non-identical timestamps, result ISE
      Assert.assertThrows(
          ISE.class,
          () -> parsed.eval(
              Util.makeBindings(
                  ImmutableMap.of(
                      "ts",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.ETERNITY,
                              100
                          )
                      ),
                      "ts1",
                      SimpleTimeSeriesContainer.createFromInstance(
                          new SimpleTimeSeries(
                              new ImplyLongArrayList(new long[]{1, 2, 3, 5}),
                              new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                              Intervals.ETERNITY,
                              100
                          )
                      )
                  ))
          )
      );
    }
  }

  @Test
  public void testNulls()
  {
    SimpleTimeSeries input = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.ETERNITY,
        100
    );
    String[] exprs = new String[]{
        "add_timeseries(ts, ts1)",
        "add_timeseries(ts1, ts)",
        "subtract_timeseries(ts, ts1)",
        "subtract_timeseries(ts1, ts)",
        "multiply_timeseries(ts, ts1)",
        "multiply_timeseries(ts1, ts)",
        "divide_timeseries(ts, ts1)",
        "divide_timeseries(ts1, ts)"
    };
    SimpleTimeSeriesContainer[] expectedResults = new SimpleTimeSeriesContainer[]{
        SimpleTimeSeriesContainer.createFromInstance(input),
        SimpleTimeSeriesContainer.createFromInstance(input),
        SimpleTimeSeriesContainer.createFromInstance(input),
        SimpleTimeSeriesContainer.createFromInstance(null),
        SimpleTimeSeriesContainer.createFromInstance(null),
        SimpleTimeSeriesContainer.createFromInstance(null),
        SimpleTimeSeriesContainer.createFromInstance(input),
        SimpleTimeSeriesContainer.createFromInstance(null)
    };

    for (int i = 0; i < exprs.length; i++) {
      Expr parsed = Parser.parse(exprs[i], MACRO_TABLE);

      // one argument null, result null
      ExprEval<?> result = parsed.eval(
          Util.makeBindings(
              ImmutableMap.of(
                  "ts",
                  SimpleTimeSeriesContainer.createFromInstance(input)
              ))
      );
      Assert.assertEquals("difference at index : " + i, expectedResults[i], result.value());
    }
  }

  @Test
  public void testEdgePointAndBucketMillisMerge()
  {
    String[] exprs = new String[]{
        "add_timeseries(ts, ts1)",
        "subtract_timeseries(ts, ts1)",
        "multiply_timeseries(ts, ts1)",
        "divide_timeseries(ts, ts1)"
    };
    double[][] edgeData = new double[][]{
        {0, 10},
        {0, 0},
        {0, 25},
        {Double.NaN, 1}
    };
    double[][] dataPoints = new double[][]{
        {2, 4, 6, 8},
        {0, 0, 0, 0},
        {1, 4, 9, 16},
        {1, 1, 1, 1}
    };
    for (int i = 0; i < exprs.length; i++) {
      Expr parsed = Parser.parse(exprs[i], MACRO_TABLE);

      // same edges, different bucket millis. result is edges added up, bucket millis as null
      ExprEval<?> result = parsed.eval(
          Util.makeBindings(
              ImmutableMap.of(
                  "ts",
                  SimpleTimeSeriesContainer.createFromInstance(
                      new SimpleTimeSeries(
                          new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                          new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                          Intervals.utc(1, 5),
                          new TimeSeries.EdgePoint(0, 0),
                          new TimeSeries.EdgePoint(5, 5),
                          100,
                          1L
                      )
                  ),
                  "ts1",
                  SimpleTimeSeriesContainer.createFromInstance(
                      new SimpleTimeSeries(
                          new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
                          new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
                          Intervals.utc(1, 5),
                          new TimeSeries.EdgePoint(0, 0),
                          new TimeSeries.EdgePoint(5, 5),
                          10,
                          null
                      )
                  )
              ))
      );

      SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
          new ImplyDoubleArrayList(dataPoints[i]),
          Intervals.utc(1, 5),
          new TimeSeries.EdgePoint(0, edgeData[i][0]),
          new TimeSeries.EdgePoint(5, edgeData[i][1]),
          100,
          null
      );
      Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);

      // different edge points, so pick the closest ones
      result = parsed.eval(
          Util.makeBindings(
              ImmutableMap.of(
                  "ts",
                  SimpleTimeSeriesContainer.createFromInstance(
                      new SimpleTimeSeries(
                          new ImplyLongArrayList(new long[]{2, 3, 4}),
                          new ImplyDoubleArrayList(new double[]{2, 3, 4}),
                          Intervals.utc(2, 5),
                          new TimeSeries.EdgePoint(0, 0),
                          new TimeSeries.EdgePoint(5, 5),
                          100,
                          1L
                      )
                  ),
                  "ts1",
                  SimpleTimeSeriesContainer.createFromInstance(
                      new SimpleTimeSeries(
                          new ImplyLongArrayList(new long[]{2, 3, 4}),
                          new ImplyDoubleArrayList(new double[]{2, 3, 4}),
                          Intervals.utc(2, 5),
                          new TimeSeries.EdgePoint(1, 1),
                          new TimeSeries.EdgePoint(7, 7),
                          10,
                          null
                      )
                  )
              ))
      );

      expectedDeltaTimeSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(new long[]{2, 3, 4}),
          new ImplyDoubleArrayList(Arrays.copyOfRange(dataPoints[i], 1, dataPoints.length)),
          Intervals.utc(2, 5),
          new TimeSeries.EdgePoint(1, 1),
          new TimeSeries.EdgePoint(5, 5),
          100,
          null
      );
      Util.expectSimpleTimeseries(result, expectedDeltaTimeSeries);
    }
  }
}
