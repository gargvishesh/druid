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
    if (timeSeries == null) {
      timeSeries = new SimpleTimeSeries(window, maxEntries);
      timeSeries.addTimeSeries(simpleTimeSeriesContainer.getSimpleTimeSeries().withWindow(window));
      timeSeries.computeSimple();
    } else {
      SimpleTimeSeries simpleTimeSeries = simpleTimeSeriesContainer.getSimpleTimeSeries().computeSimple();
      AggregateOperators.addIdenticalTimestamps(
          simpleTimeSeries.getTimestamps().getLongArray(),
          simpleTimeSeries.getDataPoints().getDoubleArray(),
          timeSeries.getTimestamps().getLongArray(),
          timeSeries.getDataPoints().getDoubleArray(),
          simpleTimeSeries.size(),
          timeSeries.size()
      );
    }
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
