/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesComplexMetricSerde;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class SimpleTimeSeriesTest extends SimpleTimeSeriesBaseTest
{
  @Override
  public SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window)
  {
    // add the provided loose data points
    SimpleTimeSeries[] seriesToMerge = new SimpleTimeSeries[seriesList.length];
    for (int i = 0; i < seriesList.length; i++) {
      seriesToMerge[i] = new SimpleTimeSeries(new ImplyLongArrayList(),
                                              new ImplyDoubleArrayList(),
                                              window,
                                              seriesList[i].getStart(),
                                              seriesList[i].getEnd(), MAX_ENTRIES);
      for (int j = 0; j < seriesList[i].size(); j++) {
        seriesToMerge[i].addDataPoint(seriesList[i].getTimestamps().getLong(j), seriesList[i].getDataPoints().getDouble(j));
      }
      seriesToMerge[i].build();
    }

    // generate a time series
    SimpleTimeSeries initSeries = new SimpleTimeSeries(window, MAX_ENTRIES);

    // add the provided timeseries
    for (SimpleTimeSeries simpleTimeSeries : seriesToMerge) {
      initSeries.addTimeSeries(simpleTimeSeries);
    }

    // build the whole thing
    initSeries.build();

    return initSeries;
  }

  @Test
  public void testSorted()
  {
    DateTime startDateTime = DateTimes.of("2020-01-01");
    SimpleTimeSeries timeSeries = new SimpleTimeSeries(SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, 100);

    // create a list of timestamps in descending order
    for (int i = 50; i >= 0; i--) {
      timeSeries.addDataPoint(startDateTime.plusHours(i).getMillis(), i);
    }

    // this will sort the resulting timestamps/datapoints by time in ascending order
    timeSeries.computeSimple();

    ImplyLongArrayList timestamps = timeSeries.getTimestamps();

    long lastTimestamp = timestamps.getLong(0);

    for (int i = 1; i < timestamps.size(); i++) {
      Assert.assertTrue(timestamps.getLong(i) > lastTimestamp);
      lastTimestamp = timestamps.getLong(i);
    }

  }
}
