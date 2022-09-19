/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.MeanByteBufferTimeSeries;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class MeanTimeSeriesBuildBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseDoubleColumnValueSelector dataSelector;
  private final MeanByteBufferTimeSeries meanByteBufferTimeSeries;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;

  public MeanTimeSeriesBuildBufferAggregator(final BaseLongColumnValueSelector timeSelector,
                                             final BaseDoubleColumnValueSelector dataSelector,
                                             final DurationGranularity durationGranularity,
                                             final Interval window,
                                             final int maxEntries)
  {
    this.timeSelector = timeSelector;
    this.dataSelector = dataSelector;
    this.meanByteBufferTimeSeries = new MeanByteBufferTimeSeries(durationGranularity, window, maxEntries);
    this.bufferToWritableMemoryCache = new BufferToWritableMemoryCache();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    meanByteBufferTimeSeries.init(bufferToWritableMemoryCache.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (dataSelector.isNull() || timeSelector.isNull()) {
      return;
    }
    meanByteBufferTimeSeries.addDataPointBuffered(bufferToWritableMemoryCache.getMemory(buf), position, timeSelector.getLong(), dataSelector.getDouble());
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return meanByteBufferTimeSeries.computeMeanBuffered(bufferToWritableMemoryCache.getMemory(buf), position);
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
