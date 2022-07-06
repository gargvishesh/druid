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
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class SimpleTimeSeriesMergeAggregator implements Aggregator
{
  private final SimpleTimeSeries timeSeries;
  private final BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector;

  public SimpleTimeSeriesMergeAggregator(
      BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector,
      Interval window,
      int maxEntries
  )
  {
    this.selector = selector;
    this.timeSeries = new SimpleTimeSeries(window, maxEntries);
  }

  @Override
  public void aggregate()
  {
    SimpleTimeSeriesContainer timeSeriesContainer = selector.getObject();

    if (timeSeriesContainer == null) {
      return;
    }

    timeSeriesContainer.pushInto(timeSeries);
  }

  @Nullable
  @Override
  public Object get()
  {
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
