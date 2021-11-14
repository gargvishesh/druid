/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation.postprocessors;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.Util;
import io.imply.druid.timeseries.interpolation.Interpolator;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class TimeWeightedAvgTimeSeriesFnTest
{
  @Test
  public void testTimeWeightedAvg1()
  {
    TimeWeightedAvgTimeSeriesFn fn = new TimeWeightedAvgTimeSeriesFn(2700001L, Interpolator.LINEAR);
    long[] timestamps = new long[]{0, 300000, 600000, 900000, 1200000, 1500000, 1800000, 1860000, 1890000, 1920000, 1950000, 1980000, 2010000, 2040000, 2070000, 2100000, 2130000, 2160000, 2400000, 2700000};
    double[] dataPoints = new double[]{4.0, 5.5, 3.0, 4.0, 3.5, 8.0, 9.0, 10.5, 11.0, 15.0, 20.0, 18.5, 17.0, 15.5, 14.0, 12.5, 11.0, 10.0, 7.0, 5.0};
    SimpleTimeSeries simpleTimeSeries = Util.makeSimpleTS(timestamps, dataPoints);
    SimpleTimeSeries avg = fn.compute(simpleTimeSeries, MAX_ENTRIES);
    Assert.assertEquals(1, avg.size());
    Assert.assertEquals(0, avg.getTimestamps().getLong(0));
    Assert.assertEquals(6.63611, avg.getDataPoints().getDouble(0), 1e-4);
  }

  @Test
  public void testTimeWeightedAvg2()
  {
    TimeWeightedAvgTimeSeriesFn fn = new TimeWeightedAvgTimeSeriesFn(60000L, Interpolator.LINEAR);
    long[] timestamps = new long[]{0, 20000, 50000};
    double[] dataPoints = new double[]{-2, 7, 4};
    SimpleTimeSeries simpleTimeSeries = Util.makeSimpleTS(timestamps, dataPoints);
    SimpleTimeSeries avg = fn.compute(simpleTimeSeries, MAX_ENTRIES);
    Assert.assertEquals(1, avg.size());
    Assert.assertEquals(0, avg.getTimestamps().getLong(0));
    Assert.assertEquals(4.16666, avg.getDataPoints().getDouble(0), 1e-4);
  }

  @Test
  public void testTimeWeightedAvg3()
  {
    TimeWeightedAvgTimeSeriesFn fn = new TimeWeightedAvgTimeSeriesFn(60000L, Interpolator.LINEAR);
    long[] timestamps = new long[]{10000, 20000, 30000, 50000};
    double[] dataPoints = new double[]{4, -3, 19, 1};
    SimpleTimeSeries simpleTimeSeries = Util.makeSimpleTS(timestamps, dataPoints);
    SimpleTimeSeries avg = fn.compute(simpleTimeSeries, MAX_ENTRIES);
    Assert.assertEquals(1, avg.size());
    Assert.assertEquals(0, avg.getTimestamps().getLong(0));
    Assert.assertEquals(5.41666, avg.getDataPoints().getDouble(0), 1e-4);
  }

  @Test
  public void testTimeWeightedAvg4()
  {
    TimeWeightedAvgTimeSeriesFn fn = new TimeWeightedAvgTimeSeriesFn(10000L, Interpolator.LINEAR);
    long[] timestamps = new long[]{11000, 12000, 21000, 31000, 51000};
    double[] dataPoints = new double[]{4, 3.3, -3, 19, 1};
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(timestamps),
                                                             new ImplyDoubleArrayList(dataPoints),
                                                             Intervals.utc(10000, 51000),
                                                             MAX_ENTRIES);
    SimpleTimeSeries avg = fn.compute(simpleTimeSeries, MAX_ENTRIES);
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(new long[]{10000, 20000, 30000, 50000}),
                                                               new ImplyDoubleArrayList(new double[]{1.2, 3.5725, 7.463333333333333, 4.9475}),
                                                               Intervals.utc(10000, 51000),
                                                               MAX_ENTRIES);
    Assert.assertEquals(expectedTimeSeries, avg);
  }
}
