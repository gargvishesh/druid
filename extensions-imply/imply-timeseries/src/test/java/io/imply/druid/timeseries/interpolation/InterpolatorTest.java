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
  @Test
  public void timeSeriesLinearInterpolatorTest()
  {
    long[] timestamps = new long[]{0, 1, 4, 6, 7};
    double[] dataPoints = new double[]{0, 1, 4, 6, 7};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(0, 8),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesLinearInterpolator = Interpolator.LINEAR;

    SimpleTimeSeries linearInterpolatedSeries = timeSeriesLinearInterpolator.interpolate(simpleTimeSeries,
                                                                                         new DurationGranularity(1, 0),
                                                                                         MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{0, 1, 2, 3, 4, 5, 6, 7};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(0, 8),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, linearInterpolatedSeries);
  }

  @Test
  public void timeSeriesLinearInterpolatorWithwindowTest()
  {
    long[] timestamps = new long[]{2, 4, 6};
    double[] dataPoints = new double[]{2, 4, 6};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(1, 8),
                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesLinearInterpolator = Interpolator.LINEAR;

    SimpleTimeSeries linearInterpolatedSeries = timeSeriesLinearInterpolator.interpolate(simpleTimeSeries,
                                                                                         new DurationGranularity(1, 0),
                                                                                         MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{1, 2, 3, 4, 5, 6, 7};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(1, 8),
                                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, linearInterpolatedSeries);
  }

  @Test
  public void timeSeriesPaddingInterpolatorTest()
  {
    long[] timestamps = new long[]{0, 1, 4, 6, 7};
    double[] dataPoints = new double[]{0, 1, 4, 6, 7};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(0, 8),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesLinearInterpolator = Interpolator.PADDING;

    SimpleTimeSeries linearInterpolatedSeries = timeSeriesLinearInterpolator.interpolate(simpleTimeSeries,
                                                                                         new DurationGranularity(1, 0),
                                                                                         MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{0, 1, 1, 1, 4, 4, 6, 7};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(0, 8),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, linearInterpolatedSeries);
  }

  @Test
  public void timeSeriesPaddingInterpolatorWithwindowTest()
  {
    long[] timestamps = new long[]{2, 4, 6};
    double[] dataPoints = new double[]{2, 4, 6};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(1, 8),
                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesPaddingInterpolator = Interpolator.PADDING;

    SimpleTimeSeries paddingInterpolatedSeries = timeSeriesPaddingInterpolator.interpolate(simpleTimeSeries,
                                                                                           new DurationGranularity(1, 0),
                                                                                           MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{0, 2, 2, 4, 4, 6, 6};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(1, 8),
                                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, paddingInterpolatedSeries);
  }

  @Test
  public void timeSeriesBackfillInterpolatorTest()
  {
    long[] timestamps = new long[]{0, 1, 4, 6, 7};
    double[] dataPoints = new double[]{0, 1, 4, 6, 7};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(0, 8),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesLinearInterpolator = Interpolator.BACKFILL;

    SimpleTimeSeries linearInterpolatedSeries = timeSeriesLinearInterpolator.interpolate(simpleTimeSeries,
                                                                                         new DurationGranularity(1, 0),
                                                                                         MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{0, 1, 4, 4, 4, 6, 6, 7};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(0, 8),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, linearInterpolatedSeries);
  }

  @Test
  public void timeSeriesBackfillInterpolatorWithwindowTest()
  {
    long[] timestamps = new long[]{2, 4, 6};
    double[] dataPoints = new double[]{2, 4, 6};

    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(1, 8),
                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                             MAX_ENTRIES);
    Interpolator timeSeriesBackfillInterpolator = Interpolator.BACKFILL;

    SimpleTimeSeries backfillInterpolatedSeries = timeSeriesBackfillInterpolator.interpolate(simpleTimeSeries,
                                                                                             new DurationGranularity(1, 0),
                                                                                             MAX_ENTRIES);
    long[] expectedTimestamps = new long[]{1, 2, 3, 4, 5, 6, 7};
    double[] expectedDataPoints = new double[]{2, 2, 4, 4, 6, 6, 8};
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(1, 8),
                                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                                             new TimeSeries.EdgePoint(8L, 8D),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, backfillInterpolatedSeries);
  }

  @Test
  public void testEmptySeriesWithwindow()
  {
    SimpleTimeSeries emptySeriesWithBounds = new SimpleTimeSeries(new ImplyLongArrayList(),
                                                                  new ImplyDoubleArrayList(),
                                                                  Intervals.utc(1, 4),
                                                                  new TimeSeries.EdgePoint(0L, 0D),
                                                                  new TimeSeries.EdgePoint(4L, 4D),
                                                                  MAX_ENTRIES);
    Interpolator linearInterpolator = Interpolator.LINEAR;
    long[] expectedTimestamps = new long[]{1};
    double[] expectedDataPoints = new double[]{1};
    SimpleTimeSeries timeSeries = linearInterpolator.interpolate(emptySeriesWithBounds,
                                                                 new DurationGranularity(1, 0),
                                                                 MAX_ENTRIES);
    SimpleTimeSeries expectedLinearInterpolatedSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                                             new ImplyDoubleArrayList(expectedDataPoints),
                                                                             Intervals.utc(1, 4),
                                                                             new TimeSeries.EdgePoint(0L, 0D),
                                                                             new TimeSeries.EdgePoint(4L, 4D),
                                                                             MAX_ENTRIES);
    Assert.assertEquals(expectedLinearInterpolatedSeries, timeSeries);
  }
}
