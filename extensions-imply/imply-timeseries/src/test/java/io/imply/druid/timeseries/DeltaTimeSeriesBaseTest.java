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
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import static io.imply.druid.timeseries.SimpleByteBufferTimeSeriesTest.VISIBLE_WINDOW;
import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public abstract class DeltaTimeSeriesBaseTest
{
  @Test
  public void testDeltaTS()
  {
    SimpleTimeSeries input = Util.makeSimpleTS(new long[]{0, 1, 2}, new double[]{0, 1, 2});
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input},
                                                    VISIBLE_WINDOW,
                                                    new DurationGranularity(2, 0));
    SimpleTimeSeries expectedSeries = Util.makeSimpleTS(new long[]{0, 2}, new double[]{1, 0});
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithDataPointsAndTimeseries()
  {
    SimpleTimeSeries input1 = Util.makeSimpleTS(new long[]{0, 1}, new double[]{0, 1});
    SimpleTimeSeries input2 = Util.makeSimpleTS(new long[]{2, 3}, new double[]{5, 2});
    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{input1, input2},
                                                    VISIBLE_WINDOW,
                                                    new DurationGranularity(2, 0));
    SimpleTimeSeries expectedSeries = Util.makeSimpleTS(new long[]{0, 2}, new double[]{1, -3});
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeseries()
  {
    SimpleTimeSeries first = Util.makeSimpleTS(new long[]{0, 1, 4, 5}, new double[]{0, 1, 4, 5});
    SimpleTimeSeries second = Util.makeSimpleTS(new long[]{1, 2, 5, 6}, new double[]{1, 2, 5, 6});
    SimpleTimeSeries third = Util.makeSimpleTS(new long[]{2, 3, 6, 7}, new double[]{2, 3, 6, 7});

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third},
                                                    VISIBLE_WINDOW,
                                                    new DurationGranularity(4, 0));

    SimpleTimeSeries expectedSeries = Util.makeSimpleTS(new long[]{0, 4}, new double[]{3, 3});
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  @Test
  public void testMergeTSWithTimeserieswindow()
  {
    Interval window = Intervals.utc(4, 12);
    SimpleTimeSeries first = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{4, 5}),
                                                  new ImplyDoubleArrayList(new double[]{4, 5}),
                                                  window,
                                                  new TimeSeries.EdgePoint(0L, 0D),
                                                  null,
                                                  MAX_ENTRIES);
    SimpleTimeSeries second = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{5, 6, 9, 10}),
                                                   new ImplyDoubleArrayList(new double[]{1, 2, 5, 6}),
                                                   window,
                                                   MAX_ENTRIES);
    SimpleTimeSeries third = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{10, 11}),
                                                  new ImplyDoubleArrayList(new double[]{2, 3}),
                                                  window,
                                                  null,
                                                  new TimeSeries.EdgePoint(12L, 12D),
                                                  MAX_ENTRIES);

    SimpleTimeSeries timeSeries = timeseriesBuilder(new SimpleTimeSeries[]{first, second, third},
                                                    window,
                                                    new DurationGranularity(4, 0));

    SimpleTimeSeries expectedSeries = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{4, 8}),
                                                           new ImplyDoubleArrayList(new double[]{-2, -2}),
                                                           window,
                                                           new TimeSeries.EdgePoint(0L, 0D),
                                                           new TimeSeries.EdgePoint(12L, 12D),
                                                           MAX_ENTRIES);
    Assert.assertEquals(2, timeSeries.size());
    Assert.assertEquals(expectedSeries, timeSeries);
  }

  public abstract SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window, DurationGranularity durationGranularity);
}
