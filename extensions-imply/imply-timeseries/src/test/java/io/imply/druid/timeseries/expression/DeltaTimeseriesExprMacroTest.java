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
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

public class DeltaTimeseriesExprMacroTest
{
  @Test
  public void testSimpleDeltaTimeseries()
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.ETERNITY,
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 1L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3}),
        new ImplyDoubleArrayList(new double[]{1, 1, 1}),
        Intervals.ETERNITY,
        100
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithBucketMillis()
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.ETERNITY,
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 10L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0}),
        new ImplyDoubleArrayList(new double[]{3}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        10L
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithBucketMillis2()
  {
    // bucket start comes out to 0, but since the window is [1, 5), the delta timeseries comes out to be empty
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.utc(1, 5),
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 10L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(),
        new ImplyDoubleArrayList(),
        Intervals.utc(1, 5),
        null,
        null,
        100,
        10L
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithEndPoint()
  {
    // have an end point in the input series, so the delta series should compute delta from
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.utc(1, 5),
        null,
        new TimeSeries.EdgePoint(5, 5),
        100,
        null
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 1L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 1, 1, 1}),
        Intervals.utc(1, 5),
        null,
        null,
        100,
        1L
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithUnsortedValues()
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{1, 2, 4, 3}),
        Intervals.ETERNITY,
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 1L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2}),
        new ImplyDoubleArrayList(new double[]{1, 2}),
        Intervals.ETERNITY,
        100
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithLongApartDataPoints()
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 33, 44}),
        new ImplyDoubleArrayList(new double[]{1, 2, 3, 4}),
        Intervals.ETERNITY,
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 5L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0, 30}),
        new ImplyDoubleArrayList(new double[]{2, 1}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        5L
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }

  @Test
  public void testSimpleDeltaTimeseries_WithLongApartDataPointsAndUnsorted()
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{1, 2, 33, 44}),
        new ImplyDoubleArrayList(new double[]{1, 2, 1.5, 3}),
        Intervals.ETERNITY,
        100
    );
    SimpleTimeSeries deltaTimeseries = DeltaTimeseriesExprMacro.buildDeltaSeries(simpleTimeSeries, 5L);
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0}),
        new ImplyDoubleArrayList(new double[]{2}),
        Intervals.ETERNITY,
        null,
        null,
        100,
        5L
    );
    Assert.assertEquals(expectedDeltaTimeSeries, deltaTimeseries);
  }
}
