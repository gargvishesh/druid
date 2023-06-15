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
import io.imply.druid.timeseries.TimeSeries;
import io.imply.druid.timeseries.aggregation.postprocessors.AggregateOperators;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SumTimeSeriesBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final Interval window;
  private final SimpleByteBufferTimeSeries simpleByteBufferTimeSeries;
  private final BufferToWritableMemoryCache bufferToWritableMemoryCache;
  private long[] tempTimestamps;
  private double[] tempDataPoints;

  public SumTimeSeriesBufferAggregator(
      BaseObjectColumnValueSelector selector,
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
    Object mergeSeriesObject = selector.getObject();
    if (mergeSeriesObject == null) {
      return;
    }
    if (!(mergeSeriesObject instanceof SimpleTimeSeriesContainer)) {
      throw new IAE("Found illegal type for timeseries column : [%s]", mergeSeriesObject.getClass());
    }

    SimpleTimeSeriesContainer simpleTimeSeriesContainer = (SimpleTimeSeriesContainer) mergeSeriesObject;
    if (simpleTimeSeriesContainer.isNull()) {
      return;
    }
    // do the aggregation
    WritableMemory memory = bufferToWritableMemoryCache.getMemory(buf);

    if (simpleByteBufferTimeSeries.isNull(memory, position)) {
      simpleTimeSeriesContainer.pushInto(
          simpleByteBufferTimeSeries,
          memory,
          position,
          window
      );
      simpleByteBufferTimeSeries.setBucketMillis(
          memory,
          position,
          simpleTimeSeriesContainer.getBucketMillis() == null ? -1 : simpleTimeSeriesContainer.getBucketMillis()
      );
      tempTimestamps = new long[simpleByteBufferTimeSeries.size(memory, position)];
      tempDataPoints = new double[simpleByteBufferTimeSeries.size(memory, position)];
    } else {
      simpleByteBufferTimeSeries.getTimestamps(memory, position, tempTimestamps);
      simpleByteBufferTimeSeries.getDataPoints(memory, position, tempDataPoints);
      SimpleTimeSeries simpleTimeSeries = simpleTimeSeriesContainer.getSimpleTimeSeries().computeSimple();

      if (simpleByteBufferTimeSeries.getBucketMillis(memory, position) != -1 &&
          !simpleTimeSeriesContainer.getBucketMillis().equals(simpleByteBufferTimeSeries.getBucketMillis(memory, position))) {
        simpleByteBufferTimeSeries.setBucketMillis(memory, position, -1);
      }

      // merge endpoints as : choose the closer end point if both are different, otherwise, add them.
      TimeSeries.EdgePoint inputStart = simpleTimeSeries.getStart();
      if (inputStart.getTimestamp() > 0) {
        TimeSeries.EdgePoint currStart = simpleByteBufferTimeSeries.getStartBuffered(memory, position);
        if (inputStart.getTimestamp() > currStart.getTimestamp()) {
          currStart.setTimestamp(inputStart.getTimestamp());
          currStart.setData(inputStart.getData());
        } else if (inputStart.getTimestamp() == currStart.getTimestamp()) {
          currStart.setData(currStart.getData() + inputStart.getData());
        }
        simpleByteBufferTimeSeries.setStartBuffered(memory, position, currStart);
      }
      TimeSeries.EdgePoint inputEnd = simpleTimeSeries.getEnd();
      if (inputEnd.getTimestamp() > 0) {
        TimeSeries.EdgePoint currEnd = simpleByteBufferTimeSeries.getEndBuffered(memory, position);
        if (inputEnd.getTimestamp() < currEnd.getTimestamp()) {
          currEnd.setTimestamp(inputEnd.getTimestamp());
          currEnd.setData(inputEnd.getData());
        } else if (inputEnd.getTimestamp() == currEnd.getTimestamp()) {
          currEnd.setData(currEnd.getData() + inputEnd.getData());
        }
        simpleByteBufferTimeSeries.setEndBuffered(memory, position, currEnd);
      }

      AggregateOperators.addIdenticalTimestamps(
          simpleTimeSeries.getTimestamps().getLongArray(),
          simpleTimeSeries.getDataPoints().getDoubleArray(),
          tempTimestamps,
          tempDataPoints,
          simpleTimeSeries.size(),
          simpleByteBufferTimeSeries.size(memory, position)
      );
      simpleByteBufferTimeSeries.setTimestamps(memory, position, tempTimestamps);
      simpleByteBufferTimeSeries.setDataPoints(memory, position, tempDataPoints);
    }
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
