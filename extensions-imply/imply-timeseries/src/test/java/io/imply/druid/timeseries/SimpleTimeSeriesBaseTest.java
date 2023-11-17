/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import static io.imply.druid.timeseries.SimpleTimeSeriesFromByteBufferAdapaterTest.VISIBLE_WINDOW;

public abstract class SimpleTimeSeriesBaseTest
{
  public static final int MAX_ENTRIES = 1 << 4;

  @Test
  public void testSimpleTS()
  {
    SimpleTimeSeries input = timeseriesBuilder(new long[]{0, 1}, new double[]{0, 0}, null, null, VISIBLE_WINDOW);
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = timeseriesBuilder(new long[]{0, 1}, new double[]{0, 0}, null, null, VISIBLE_WINDOW);
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testSimpleTSWithEdges()
  {
    Interval window = Intervals.utc(2, 3);
    SimpleTimeSeries actualSeries = timeseriesBuilder(new long[]{0, 1, 2, 3, 4}, new double[]{0, 1, 2, 3, 4}, null, null, window);
    SimpleTimeSeries expectedSeries = timeseriesBuilder(
        new long[]{2},
        new double[]{2},
        new TimeSeries.EdgePoint(1, 1),
        new TimeSeries.EdgePoint(3, 3),
        window
    );
    Assert.assertEquals(expectedSeries, actualSeries);
  }

  @Test
  public void testMergeTSWithDataPointsAndTimeseries()
  {
    SimpleTimeSeries simpleTimeSeries = timeseriesBuilder(new long[]{1}, new double[]{1}, null, null, VISIBLE_WINDOW);
    SimpleTimeSeries input1 = timeseriesBuilder(new long[]{0, 5}, new double[]{0, 0}, null, null, VISIBLE_WINDOW);
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input1, simpleTimeSeries}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{0, 1, 5}),
        new ImplyDoubleArrayList(new double[]{0, 1, 0}),
        VISIBLE_WINDOW,
        MAX_ENTRIES
    );
    Assert.assertEquals(3, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeseries()
  {
    SimpleTimeSeries first = timeseriesBuilder(new long[]{1, 4}, new double[]{1, 4}, null, null, VISIBLE_WINDOW);
    SimpleTimeSeries second = timeseriesBuilder(new long[]{2, 5}, new double[]{2, 5}, null, null, VISIBLE_WINDOW);
    SimpleTimeSeries third = timeseriesBuilder(new long[]{3, 6}, new double[]{10, 15}, null, null, VISIBLE_WINDOW);

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = timeseriesBuilder(
        new long[]{1, 2, 3, 4, 5, 6},
        new double[]{1, 2, 10, 4, 5, 15},
        null,
        null,
        VISIBLE_WINDOW
    );
    Assert.assertEquals(6, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeseriesWindow()
  {
    Interval window = Intervals.utc(2, 5);
    SimpleTimeSeries first = timeseriesBuilder(
        new long[]{4},
        new double[]{4},
        new TimeSeries.EdgePoint(1L, 1D),
        null,
        window
    );
    SimpleTimeSeries second = timeseriesBuilder(
        new long[]{2},
        new double[]{2},
        null,
        null,
        window
    );
    SimpleTimeSeries third = timeseriesBuilder(
        new long[]{3},
        new double[]{10},
        null,
        new TimeSeries.EdgePoint(5L, 5D),
        window
    );

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third}, window);
    SimpleTimeSeries expectedSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(new long[]{2, 3, 4}),
        new ImplyDoubleArrayList(new double[]{2, 10, 4}),
        window,
        new TimeSeries.EdgePoint(1L, 1D),
        new TimeSeries.EdgePoint(5L, 5D),
        MAX_ENTRIES,
        1L
    );
    Assert.assertEquals(3, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testTSWithWindows()
  {
    Interval[] windowList = new Interval[]{
        Intervals.utc(1, 6),
        Intervals.utc(3, 6),
        Intervals.utc(1, 4),
        Intervals.utc(2, 5)
    };
    long[][] timestampsList = new long[][] {
        {1, 2, 3, 4, 5},
        {3, 4, 5},
        {1, 2, 3},
        {2, 3, 4},
    };
    double[][] dataPointsList = new double[][] {
        {1, 2, 3, 4, 5},
        {3, 4, 5},
        {1, 2, 3},
        {2, 3, 4},
    };
    TimeSeries.EdgePoint[] startsList = new TimeSeries.EdgePoint[] {
        null,
        new TimeSeries.EdgePoint(2L, 2D),
        null,
        new TimeSeries.EdgePoint(1L, 1D)
    };
    TimeSeries.EdgePoint[] endsList = new TimeSeries.EdgePoint[] {
        null,
        null,
        new TimeSeries.EdgePoint(4L, 4D),
        new TimeSeries.EdgePoint(5L, 5D)
    };

    for (int i = 0; i < windowList.length; i++) {
      SimpleTimeSeries simpleTimeSeries = timeseriesBuilder(
          new long[]{1, 2, 3, 4, 5},
          new double[]{1, 2, 3, 4, 5},
          startsList[i],
          endsList[i],
          windowList[i]
      );
      SimpleTimeSeries expectedSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(timestampsList[i]),
          new ImplyDoubleArrayList(dataPointsList[i]),
          windowList[i],
          startsList[i],
          endsList[i],
          MAX_ENTRIES,
          1L
      );
      Assert.assertEquals(expectedSeries, simpleTimeSeries);
    }
  }

  @Test
  public void testMergeEmptySeriesWithEdgesAndNonEmptySeries()
  {
    Interval window = Intervals.utc(1, 4);
    SimpleTimeSeries emptySeries = timeseriesBuilder(
        new long[]{},
        new double[]{},
        new TimeSeries.EdgePoint(0, 0),
        new TimeSeries.EdgePoint(4, 4),
        window
    );
    SimpleTimeSeries nonEmptySeries = timeseriesBuilder(
        new long[]{1, 2, 3},
        new double[]{1, 2, 3},
        null,
        null,
        window
    );
    SimpleTimeSeries actualSeries = timeseriesBuilder(new SimpleTimeSeries[]{emptySeries, nonEmptySeries}, window);
    SimpleTimeSeries expectedSeries = timeseriesBuilder(
        new long[]{1, 2, 3},
        new double[]{1, 2, 3},
        new TimeSeries.EdgePoint(0, 0),
        new TimeSeries.EdgePoint(4, 4),
        window
    );
    Assert.assertEquals(expectedSeries, actualSeries);
  }

  /**
   * timeseries objects are merged in the order they are provided in the seriesList
   */
  public abstract SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window);

  public abstract SimpleTimeSeries timeseriesBuilder(
      long[] timestamps,
      double[] dataPoints,
      TimeSeries.EdgePoint left,
      TimeSeries.EdgePoint right,
      Interval window
  );
}
