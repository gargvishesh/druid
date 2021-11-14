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
import org.joda.time.Interval;

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
}
