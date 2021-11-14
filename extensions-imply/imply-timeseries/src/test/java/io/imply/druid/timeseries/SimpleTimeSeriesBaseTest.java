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

  public abstract SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window);
}
