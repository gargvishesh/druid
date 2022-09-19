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
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector;
  private final Interval window;
  private final SimpleByteBufferTimeSeries simpleByteBufferTimeSeries;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;

  public SimpleTimeSeriesMergeBufferAggregator(
      BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector,
      Interval window,
      int maxEntries
  )
  {
    this.selector = selector;
    this.window = window;
    this.simpleByteBufferTimeSeries = new SimpleByteBufferTimeSeries(window, maxEntries);
    this.bufferToWritableMemoryCache = new BufferToWritableMemoryCache();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    simpleByteBufferTimeSeries.init(bufferToWritableMemoryCache.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    SimpleTimeSeriesContainer mergeSeriesContainer = selector.getObject();

    // This should never *BE* null since any null object would be contained
    if (mergeSeriesContainer == null || mergeSeriesContainer.isNull()) {
      return;
    }

    mergeSeriesContainer.pushInto(simpleByteBufferTimeSeries, bufferToWritableMemoryCache.getMemory(buf), position, window);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    SimpleTimeSeries simpleTimeSeries =
        simpleByteBufferTimeSeries.computeSimpleBuffered(bufferToWritableMemoryCache.getMemory(buf), position);

    return SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries);
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
