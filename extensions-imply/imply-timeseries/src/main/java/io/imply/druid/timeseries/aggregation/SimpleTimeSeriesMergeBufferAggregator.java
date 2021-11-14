/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.SimpleByteBufferTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeries;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<SimpleTimeSeries> selector;
  private final SimpleByteBufferTimeSeries simpleByteBufferTimeSeries;
  private final TimeSeriesBufferAggregatorHelper timeSeriesBufferAggregatorHelper;

  public SimpleTimeSeriesMergeBufferAggregator(final BaseObjectColumnValueSelector<SimpleTimeSeries> selector,
                                               final Interval window,
                                               final int maxEntries)
  {
    this.selector = selector;
    this.simpleByteBufferTimeSeries = new SimpleByteBufferTimeSeries(window, maxEntries);
    this.timeSeriesBufferAggregatorHelper = new TimeSeriesBufferAggregatorHelper();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    simpleByteBufferTimeSeries.init(timeSeriesBufferAggregatorHelper.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final SimpleTimeSeries mergeSeries = selector.getObject();
    if (mergeSeries == null) {
      return;
    }
    simpleByteBufferTimeSeries.mergeSeriesBuffered(timeSeriesBufferAggregatorHelper.getMemory(buf), position, mergeSeries);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return simpleByteBufferTimeSeries.computeSimpleBuffered(timeSeriesBufferAggregatorHelper.getMemory(buf), position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {

  }
}
