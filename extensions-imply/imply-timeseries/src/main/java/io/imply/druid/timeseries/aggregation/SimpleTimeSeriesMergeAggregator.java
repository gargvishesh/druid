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
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class SimpleTimeSeriesMergeAggregator implements Aggregator
{
  private final SimpleTimeSeries timeSeries;
  private final BaseObjectColumnValueSelector<SimpleTimeSeries> selector;

  public SimpleTimeSeriesMergeAggregator(final BaseObjectColumnValueSelector<SimpleTimeSeries> selector,
                                         final Interval window,
                                         final int maxEntries)
  {
    this.selector = selector;
    this.timeSeries = new SimpleTimeSeries(window, maxEntries);
  }

  @Override
  public void aggregate()
  {
    SimpleTimeSeries mergeSeries = selector.getObject();
    if (mergeSeries == null) {
      return;
    }
    timeSeries.addTimeSeries(mergeSeries.withWindow(timeSeries.getwindow()));
  }

  @Nullable
  @Override
  public Object get()
  {
    return timeSeries.computeSimple();
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
