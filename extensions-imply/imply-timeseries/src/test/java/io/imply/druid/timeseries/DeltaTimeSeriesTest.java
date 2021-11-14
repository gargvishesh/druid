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
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;

public class DeltaTimeSeriesTest extends DeltaTimeSeriesBaseTest
{
  @Override
  public SimpleTimeSeries timeseriesBuilder(SimpleTimeSeries[] seriesList, Interval window, DurationGranularity durationGranularity)
  {
    DeltaTimeSeries[] seriesToMerge = new DeltaTimeSeries[seriesList.length];
    for (int i = 0; i < seriesList.length; i++) {
      seriesToMerge[i] = new DeltaTimeSeries(new ImplyLongArrayList(),
                                             new ImplyDoubleArrayList(),
                                             durationGranularity,
                                             window,
                                             seriesList[i].getStart(),
                                             seriesList[i].getEnd(),
                                             MAX_ENTRIES);
      for (int j = 0; j < seriesList[i].size(); j++) {
        seriesToMerge[i].addDataPoint(seriesList[i].getTimestamps().getLong(j), seriesList[i].getDataPoints().getDouble(j));
      }
      seriesToMerge[i].build();
    }

    DeltaTimeSeries initSeries = new DeltaTimeSeries(durationGranularity, window, MAX_ENTRIES);
    for (DeltaTimeSeries meanTimeSeries : seriesToMerge) {
      initSeries.addTimeSeries(meanTimeSeries);
    }

    // build the whole thing
    initSeries.build();

    return initSeries.computeSimple();
  }
}
