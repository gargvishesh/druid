/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.DeltaByteBufferTimeSeries;
import io.imply.druid.timeseries.DeltaTimeSeries;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DeltaTimeSeriesMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<DeltaTimeSeries> selector;
  private final DeltaByteBufferTimeSeries deltaByteBufferTimeSeries;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;

  public DeltaTimeSeriesMergeBufferAggregator(final BaseObjectColumnValueSelector<DeltaTimeSeries> selector,
                                             final DurationGranularity durationGranularity,
                                             final Interval window,
                                             final int maxEntries)
  {
    this.selector = selector;
    this.deltaByteBufferTimeSeries = new DeltaByteBufferTimeSeries(durationGranularity, window, maxEntries);
    this.bufferToWritableMemoryCache = new BufferToWritableMemoryCache();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    deltaByteBufferTimeSeries.init(bufferToWritableMemoryCache.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final DeltaTimeSeries mergeSeries = selector.getObject();
    if (mergeSeries == null) {
      return;
    }
    deltaByteBufferTimeSeries.mergeSeriesBuffered(bufferToWritableMemoryCache.getMemory(buf), position, mergeSeries);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return deltaByteBufferTimeSeries.computeDeltaBuffered(bufferToWritableMemoryCache.getMemory(buf), position);
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
