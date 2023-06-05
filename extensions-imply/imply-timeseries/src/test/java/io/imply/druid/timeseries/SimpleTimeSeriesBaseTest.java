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

import static io.imply.druid.timeseries.SimpleByteBufferTimeSeriesTest.VISIBLE_WINDOW;

public abstract class SimpleTimeSeriesBaseTest
{
  public static final int MAX_ENTRIES = 1 << 4;

  @Test
  public void testSimpleTS()
  {
    SimpleTimeSeries input = Util.makeSimpleTS(new long[]{0, 1}, new double[]{0, 0});
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = Util.makeSimpleTS(new long[]{0, 1}, new double[]{0, 0});
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithDataPointsAndTimeseries()
  {
    SimpleTimeSeries simpleTimeSeries = Util.makeSimpleTS(new long[]{1}, new double[]{1});
    SimpleTimeSeries input1 = Util.makeSimpleTS(new long[]{0, 5}, new double[]{0, 0});
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input1, simpleTimeSeries}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{0, 1, 5}),
                                                           new ImplyDoubleArrayList(new double[]{0, 1, 0}),
                                                           VISIBLE_WINDOW,
                                                           MAX_ENTRIES);
    Assert.assertEquals(3, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeseries()
  {
    SimpleTimeSeries first = Util.makeSimpleTS(new long[]{1, 4}, new double[]{1, 4});
    SimpleTimeSeries second = Util.makeSimpleTS(new long[]{2, 5}, new double[]{2, 5});
    SimpleTimeSeries third = Util.makeSimpleTS(new long[]{3, 6}, new double[]{10, 15});

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third}, VISIBLE_WINDOW);
    SimpleTimeSeries expectedSeries = Util.makeSimpleTS(new long[]{1, 2, 3, 4, 5, 6}, new double[]{1, 2, 10, 4, 5, 15});
    Assert.assertEquals(6, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeserieswindow()
  {
    Interval window = Intervals.utc(2, 5);
    SimpleTimeSeries first = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{4}),
                                                  new ImplyDoubleArrayList(new double[]{4}),
                                                  window,
                                                  new TimeSeries.EdgePoint(1L, 1D),
                                                  null,
                                                  MAX_ENTRIES);
    SimpleTimeSeries second = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{2}),
                                                   new ImplyDoubleArrayList(new double[]{2}),
                                                   window,
                                                   MAX_ENTRIES);
    SimpleTimeSeries third = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{3}),
                                                  new ImplyDoubleArrayList(new double[]{10}),
                                                  window,
                                                  null,
                                                  new TimeSeries.EdgePoint(5L, 5D),
                                                  MAX_ENTRIES);

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third}, window);
    SimpleTimeSeries expectedSeries = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{2, 3, 4}),
                                                           new ImplyDoubleArrayList(new double[]{2, 10, 4}),
                                                           window,
                                                           new TimeSeries.EdgePoint(1L, 1D),
                                                           new TimeSeries.EdgePoint(5L, 5D),
                                                           MAX_ENTRIES);
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
      SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(windowList[i], 100);
      for (int j = 1; j < 6; j++) {
        simpleTimeSeries.addDataPoint(j, j);
      }
      SimpleTimeSeries expectedSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(timestampsList[i]),
          new ImplyDoubleArrayList(dataPointsList[i]),
          windowList[i],
          startsList[i],
          endsList[i],
          100
      );
      Assert.assertEquals(expectedSeries, simpleTimeSeries);
    }
  }

  public abstract SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window);
}
