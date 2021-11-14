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
import io.imply.druid.timeseries.MeanTimeSeries;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class MeanTimeSeriesMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<MeanTimeSeries> selector;
  private final MeanByteBufferTimeSeries meanByteBufferTimeSeries;
  private final TimeSeriesBufferAggregatorHelper timeSeriesBufferAggregatorHelper;

  public MeanTimeSeriesMergeBufferAggregator(final BaseObjectColumnValueSelector<MeanTimeSeries> selector,
                                             final DurationGranularity durationGranularity,
                                             final Interval window,
                                             final int maxEntries)
  {
    this.selector = selector;
    this.meanByteBufferTimeSeries = new MeanByteBufferTimeSeries(durationGranularity, window, maxEntries);
    this.timeSeriesBufferAggregatorHelper = new TimeSeriesBufferAggregatorHelper();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    meanByteBufferTimeSeries.init(timeSeriesBufferAggregatorHelper.getMemory(buf), position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final MeanTimeSeries mergeSeries = selector.getObject();
    if (mergeSeries == null) {
      return;
    }
    meanByteBufferTimeSeries.mergeSeriesBuffered(timeSeriesBufferAggregatorHelper.getMemory(buf), position, mergeSeries);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return meanByteBufferTimeSeries.computeMeanBuffered(timeSeriesBufferAggregatorHelper.getMemory(buf), position);
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
