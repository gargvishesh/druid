/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.DownsampledSumTimeSeriesFromByteBufferAdapter;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DownsampledSumTimeSeriesMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector;
  private final DownsampledSumTimeSeriesFromByteBufferAdapter downsampledSumTimeSeriesFromByteBufferAdapter;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;

  public DownsampledSumTimeSeriesMergeBufferAggregator(
      final BaseObjectColumnValueSelector<SimpleTimeSeriesContainer> selector,
      final DurationGranularity durationGranularity,
      final Interval window,
      final int maxEntries
  )
  {
    this.selector = selector;
    this.downsampledSumTimeSeriesFromByteBufferAdapter = new DownsampledSumTimeSeriesFromByteBufferAdapter(durationGranularity, window, maxEntries);
    this.bufferToWritableMemoryCache = new BufferToWritableMemoryCache();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    downsampledSumTimeSeriesFromByteBufferAdapter.init(bufferToWritableMemoryCache.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final SimpleTimeSeriesContainer mergeSeries = selector.getObject();
    if (mergeSeries == null) {
      return;
    }
    downsampledSumTimeSeriesFromByteBufferAdapter.mergeSeriesBuffered(
        bufferToWritableMemoryCache.getMemory(buf),
        position,
        DownsampledSumTimeSeriesAggregatorFactory.makeDownsampledSumTimeSeriesFromObject(
            mergeSeries,
            downsampledSumTimeSeriesFromByteBufferAdapter.getWindow(),
            downsampledSumTimeSeriesFromByteBufferAdapter.getMaxEntries(),
            downsampledSumTimeSeriesFromByteBufferAdapter.getTimeBucketGranularity().getDurationMillis()
        )
    );
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return SimpleTimeSeriesContainer.createFromInstance(
        downsampledSumTimeSeriesFromByteBufferAdapter.computeDownsampledSumBuffered(bufferToWritableMemoryCache.getMemory(buf), position)
                                                     .computeSimple()
    );
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
