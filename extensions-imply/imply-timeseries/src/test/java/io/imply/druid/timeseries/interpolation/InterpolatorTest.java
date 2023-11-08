/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.interpolation;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.junit.Assert;
import org.junit.Test;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class InterpolatorTest
{
  private static final long[][] TIMESTAMPS_LIST = new long[][]{
      {0, 1, 2, 3, 4, 5, 6, 7},
      {2, 3, 4, 5, 6, 7},
      {1, 2, 3, 4, 5, 6},
      {2, 3, 4, 5, 6},
      {2, 4, 6},
      {1, 2, 5, 6, 7}
  };
  private static final double[][] DATA_POINTS_LIST = new double[][]{
      {0, 1, 2, 3, 4, 5, 6, 7},
      {2, 3, 4, 5, 6, 7},
      {1, 2, 3, 4, 5, 6},
      {2, 3, 4, 5, 6},
      {2, 4, 6},
      {1, 2, 5, 6, 7}
  };

  @Test
  public void timeSeriesLinearInterpolatorTest()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {2, 2, 2, 3, 4, 5, 6, 7},
        {1, 1, 2, 3, 4, 5, 6, 6},
        {2, 2, 2, 3, 4, 5, 6, 6},
        {2, 2, 2, 3, 4, 5, 6, 6},
        {1, 1, 2, 3, 4, 5, 6, 7}
    };
    runOverInputs(
        Interpolator.LINEAR,
        new DurationGranularity(1, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesLinearInterpolatorWithWindowTest()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5, 6}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5, 5},
        {3, 3, 4, 5, 6},
        {3, 3, 4, 5, 5}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.LINEAR,
        new DurationGranularity(2, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesLinearInterpolator_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 2, 4, 6},
        {2, 2, 4, 6},
        {1, 2, 4, 6},
        {2, 2, 4, 6},
        {2, 2, 4, 6},
        {1, 2, 4, 6}
    };
    runOverInputs(
        Interpolator.LINEAR,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesLinearInterpolatorWithWindowTest_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {2, 4, 6},
        {2, 4, 5},
        {3, 4, 6},
        {3, 4, 5}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.LINEAR,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesPaddingInterpolatorTest()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {2, 3, 4, 5, 6, 7},
        {1, 2, 3, 4, 5, 6, 7},
        {2, 3, 4, 5, 6, 7},
        {2, 3, 4, 5, 6, 7},
        {1, 2, 3, 4, 5, 6, 7}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {2, 3, 4, 5, 6, 7},
        {1, 2, 3, 4, 5, 6, 6},
        {2, 3, 4, 5, 6, 6},
        {2, 2, 4, 4, 6, 6},
        {1, 2, 2, 2, 5, 6, 7}
    };
    runOverInputs(
        Interpolator.PADDING,
        new DurationGranularity(1, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesPaddingInterpolatorWithWindowTest()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5, 6},
        {3, 4, 5, 6},
        {3, 4, 5, 6}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {0, 3, 4, 5, 5},
        {0, 3, 4, 5, 5},
        {3, 4, 5, 5},
        {3, 4, 5, 5}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.PADDING,
        new DurationGranularity(2, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesPaddingInterpolator_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 4, 6},
        {2, 2, 6}
    };
    runOverInputs(
        Interpolator.PADDING,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesPaddingInterpolatorWithWindowTest_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 4, 6},
        {2, 4, 6},
        {4, 6},
        {4, 6}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {0, 4, 5},
        {0, 4, 5},
        {4, 5},
        {4, 5}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.PADDING,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesBackfillInterpolatorTest()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6, 7},
        {0, 1, 2, 3, 4, 5, 6},
        {0, 1, 2, 3, 4, 5, 6},
        {0, 1, 2, 3, 4, 5, 6},
        {0, 1, 2, 3, 4, 5, 6, 7}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 1, 2, 3, 4, 5, 6, 7},
        {2, 2, 2, 3, 4, 5, 6, 7},
        {1, 1, 2, 3, 4, 5, 6},
        {2, 2, 2, 3, 4, 5, 6},
        {2, 2, 2, 4, 4, 6, 6},
        {1, 1, 2, 5, 5, 5, 6, 7}
    };
    runOverInputs(
        Interpolator.BACKFILL,
        new DurationGranularity(1, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesBackfillInterpolatorWithWindowTest()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5},
        {2, 3, 4, 5, 6},
        {2, 3, 4, 5}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {3, 3, 4, 5, 8},
        {3, 3, 4, 5},
        {3, 3, 4, 5, 8},
        {3, 3, 4, 5}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.BACKFILL,
        new DurationGranularity(2, 0),
        false,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesBackfillInterpolator_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][]{
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6},
        {0, 2, 4, 6}
    };
    double[][] expectedDataPointsList = new double[][]{
        {0, 2, 4, 6},
        {2, 2, 4, 6},
        {1, 2, 4, 6},
        {2, 2, 4, 6},
        {2, 2, 4, 6},
        {1, 2, 5, 6}
    };
    runOverInputs(
        Interpolator.BACKFILL,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  @Test
  public void timeSeriesBackfillInterpolatorWithWindowTest_OnlyBoundaries()
  {
    long[][] expectedTimestampsList = new long[][] {
        {2, 4, 6},
        {2, 4},
        {2, 4, 6},
        {2, 4}
    };
    // input {3, 4, 5}
    double[][] expectedDataPointsList = new double[][] {
        {3, 4, 8},
        {3, 4},
        {3, 4, 8},
        {3, 4}
    };
    runOverInputs_WithEdgePointsAndWindowOneToEight(
        Interpolator.BACKFILL,
        new DurationGranularity(2, 0),
        true,
        expectedTimestampsList,
        expectedDataPointsList
    );
  }

  private void runOverInputs(
      Interpolator interpolator,
      DurationGranularity interpolationBucket,
      boolean keepBoundaries,
      long[][] expectedTimestampsList,
      double[][] expectedDataPointsList
  )
  {
    assert TIMESTAMPS_LIST.length == expectedTimestampsList.length;
    for (int i = 0; i < TIMESTAMPS_LIST.length; i++) {
      long[] timestamps = TIMESTAMPS_LIST[i];
      double[] dataPoints = DATA_POINTS_LIST[i];

      SimpleTimeSeries inputSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(timestamps),
          new ImplyDoubleArrayList(dataPoints),
          Intervals.utc(0, 8),
          MAX_ENTRIES
      );
      SimpleTimeSeries interpolatedSeries = interpolator.interpolate(
          inputSeries,
          interpolationBucket,
          MAX_ENTRIES,
          keepBoundaries
      ).computeSimple();
      SimpleTimeSeries expectedSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(expectedTimestampsList[i]),
          new ImplyDoubleArrayList(expectedDataPointsList[i]),
          Intervals.utc(0, 8),
          null,
          null,
          MAX_ENTRIES,
          keepBoundaries ? interpolationBucket.getDurationMillis() : 1L
      );
      Assert.assertEquals(String.valueOf(i), interpolatedSeries, expectedSeries);
    }
  }

  private void runOverInputs_WithEdgePointsAndWindowOneToEight(
      Interpolator interpolator,
      DurationGranularity durationGranularity,
      boolean keepBoundaries,
      long[][] expectedTimestampsList,
      double[][] expectedDataPointsList
  )
  {
    long[] timestamps = new long[]{3, 4, 5};
    double[] dataPoints = new double[]{3, 4, 5};
    TimeSeries.EdgePoint[] starts = new TimeSeries.EdgePoint[] {
        new TimeSeries.EdgePoint(0L, 0D),
        new TimeSeries.EdgePoint(0L, 0D),
        null,
        null
    };
    TimeSeries.EdgePoint[] ends = new TimeSeries.EdgePoint[] {
        new TimeSeries.EdgePoint(8L, 8D),
        null,
        new TimeSeries.EdgePoint(8L, 8D),
        null
    };

    for (int i = 0; i < starts.length; i++) {
      SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(timestamps),
          new ImplyDoubleArrayList(dataPoints),
          Intervals.utc(1, 8),
          starts[i],
          ends[i],
          MAX_ENTRIES,
          1L
      );

      SimpleTimeSeries linearInterpolatedSeries = interpolator.interpolate(
          simpleTimeSeries,
          durationGranularity,
          MAX_ENTRIES,
          keepBoundaries
      ).computeSimple();
      SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(expectedTimestampsList[i]),
          new ImplyDoubleArrayList(expectedDataPointsList[i]),
          Intervals.utc(1, 8),
          starts[i],
          ends[i],
          MAX_ENTRIES,
          keepBoundaries ? durationGranularity.getDurationMillis() : 1L
      );
      Assert.assertEquals(String.valueOf(i), expectedLinearInterpolatedSeries, linearInterpolatedSeries);
    }
  }

  @Test
  public void testEmptySeriesWithWindow()
  {
    SimpleTimeSeries emptySeriesWithBounds = new SimpleTimeSeries(
        new ImplyLongArrayList(),
        new ImplyDoubleArrayList(),
        Intervals.utc(1, 4),
        new TimeSeries.EdgePoint(0L, 0D),
        new TimeSeries.EdgePoint(4L, 4D),
        MAX_ENTRIES,
        1L
    );
    Interpolator linearInterpolator = Interpolator.LINEAR;
    long[] expectedTimestamps = new long[]{1};
    double[] expectedDataPoints = new double[]{1};
    SimpleTimeSeries timeSeries = linearInterpolator.interpolate(
        emptySeriesWithBounds,
        new DurationGranularity(1, 0),
        MAX_ENTRIES,
        false
    ).computeSimple();
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDataPoints),
        Intervals.utc(1, 4),
        new TimeSeries.EdgePoint(0L, 0D),
        new TimeSeries.EdgePoint(4L, 4D),
        MAX_ENTRIES,
        1L
    );
    Assert.assertEquals(expectedLinearInterpolatedSeries, timeSeries);
  }
}
