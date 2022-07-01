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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesBuildBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseDoubleColumnValueSelector dataSelector;
  private final SimpleByteBufferTimeSeries simpleByteBufferTimeSeries;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;

  public SimpleTimeSeriesBuildBufferAggregator(
      final BaseLongColumnValueSelector timeSelector,
      final BaseDoubleColumnValueSelector dataSelector,
      final Interval window,
      final int maxEntries
  )
  {
    this.timeSelector = timeSelector;
    this.dataSelector = dataSelector;
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
    if (dataSelector.isNull() || timeSelector.isNull()) {
      return;
    }
    simpleByteBufferTimeSeries.addDataPointBuffered(
        bufferToWritableMemoryCache.getMemory(buf),
        position,
        timeSelector.getLong(),
        dataSelector.getDouble()
    );
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return simpleByteBufferTimeSeries.computeSimpleBuffered(bufferToWritableMemoryCache.getMemory(buf), position);
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
