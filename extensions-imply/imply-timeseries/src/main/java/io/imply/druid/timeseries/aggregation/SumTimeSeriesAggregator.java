/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.SimpleTimeSeriesUtils;
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.AggregateOperators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class SumTimeSeriesAggregator implements Aggregator
{
  private SimpleTimeSeries timeSeries;
  private final BaseObjectColumnValueSelector selector;
  @Nullable
  private final Interval window;
  private final int maxEntries;

  public SumTimeSeriesAggregator(
      BaseObjectColumnValueSelector selector,
      Interval window,
      int maxEntries
  )
  {
    this.selector = selector;
    this.window = window;
    this.maxEntries = maxEntries;
  }

  @Override
  public void aggregate()
  {
    Object timeseriesObject = selector.getObject();
    if (timeseriesObject == null) {
      return;
    }
    if (!(timeseriesObject instanceof SimpleTimeSeriesContainer)) {
      throw new ISE("Found illegal type for timeseries column : [%s]", timeseriesObject.getClass());
    }

    SimpleTimeSeriesContainer simpleTimeSeriesContainer;
    simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) timeseriesObject;
    if (simpleTimeSeriesContainer.isNull()) {
      return;
    }
    // do the aggregation
    SimpleTimeSeries simpleTimeSeries;
    if (window == null) {
      simpleTimeSeries = simpleTimeSeriesContainer.getSimpleTimeSeries().computeSimple().copyWithMaxEntries(maxEntries);
    } else {
      simpleTimeSeries = simpleTimeSeriesContainer.getSimpleTimeSeries()
                                                  .computeSimple()
                                                  .copyWithWindowAndMaxEntries(window, maxEntries);
    }
    if (timeSeries == null) {
      timeSeries = simpleTimeSeries;
    } else {
      timeSeries = combineTimeSeries(timeSeries, simpleTimeSeries);
    }
  }

  public static SimpleTimeSeries combineTimeSeries(SimpleTimeSeries timeSeries, SimpleTimeSeries simpleTimeSeries)
  {
    SimpleTimeSeriesUtils.checkMatchingWindows(timeSeries, simpleTimeSeries, "sum_timeseries");
    if (timeSeries.getBucketMillis() != null &&
        !simpleTimeSeries.getBucketMillis().equals(timeSeries.getBucketMillis())) {
      timeSeries.setBucketMillis(null);
    }
    // merge endpoints as : choose the closer end point if both are different, otherwise, add them.
    TimeSeries.EdgePoint inputStart = simpleTimeSeries.getStart();
    if (inputStart.getTimestamp() != -1) {
      TimeSeries.EdgePoint currStart = timeSeries.getStart();
      if (inputStart.getTimestamp() > currStart.getTimestamp()) {
        currStart.setTimestamp(inputStart.getTimestamp());
        currStart.setData(inputStart.getData());
      } else if (inputStart.getTimestamp() == currStart.getTimestamp()) {
        currStart.setData(currStart.getData() + inputStart.getData());
      }
    }
    TimeSeries.EdgePoint inputEnd = simpleTimeSeries.getEnd();
    if (inputEnd.getTimestamp() != -1) {
      TimeSeries.EdgePoint currEnd = timeSeries.getEnd();
      if (inputEnd.getTimestamp() < currEnd.getTimestamp()) {
        currEnd.setTimestamp(inputEnd.getTimestamp());
        currEnd.setData(inputEnd.getData());
      } else if (inputEnd.getTimestamp() == currEnd.getTimestamp()) {
        currEnd.setData(currEnd.getData() + inputEnd.getData());
      }
    }
    AggregateOperators.addIdenticalTimestamps(
        simpleTimeSeries.getTimestamps().getLongArray(),
        simpleTimeSeries.getDataPoints().getDoubleArray(),
        timeSeries.getTimestamps().getLongArray(),
        timeSeries.getDataPoints().getDoubleArray(),
        simpleTimeSeries.size(),
        timeSeries.size()
    );
    return timeSeries;
  }

  @Nullable
  @Override
  public Object get()
  {
    if (timeSeries == null) {
      return SimpleTimeSeriesContainer.createFromInstance(null);
    }
    return SimpleTimeSeriesContainer.createFromInstance(timeSeries.computeSimple());
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {

  }
}
