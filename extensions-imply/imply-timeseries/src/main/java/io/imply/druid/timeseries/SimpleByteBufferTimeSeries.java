/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.RE;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;

public class SimpleByteBufferTimeSeries extends ByteBufferTimeSeries<SimpleTimeSeries>
{
  public SimpleByteBufferTimeSeries(Interval window, int maxEntries)
  {
    super(window, maxEntries);
  }

  @Override
  void internalAddDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data)
  {
    int count = mem.getInt(buffStartPosition);
    if (count >= getMaxEntries()) {
      throw new RE("Exceeded the max entries allowed (currentEntries = %d, maxEntries = %d)", count, getMaxEntries());
    }
    mem.putLong(buffStartPosition + DATA_OFFSET + count * Long.BYTES, timestamp);
    mem.putDouble(buffStartPosition + DATA_OFFSET + (getMaxEntries() * Long.BYTES) + count * Double.BYTES, data);
    mem.putInt(buffStartPosition, count + 1);
  }

  @Override
  void internalMergeSeriesBuffered(WritableMemory mem, int buffStartPosition, SimpleTimeSeries mergeSeries)
  {
    int currSize = size(mem, buffStartPosition);
    if (currSize + mergeSeries.size() >= getMaxEntries()) {
      throw new RE("Merging will exceed the max entries allowed (currentEntries = %d, mergeSeriesSize = %d, maxEntries = %d)",
                   currSize,
                   mergeSeries.size(),
                   getMaxEntries());
    }

    // bulk write timestamps
    mem.putLongArray(buffStartPosition + DATA_OFFSET + currSize * Long.BYTES,
                     mergeSeries.getTimestamps().getLongArray(),
                     0,
                     mergeSeries.getTimestamps().size());

    // bulk write data points
    mem.putDoubleArray(buffStartPosition + DATA_OFFSET + (getMaxEntries() * Long.BYTES) + currSize * Double.BYTES,
                       mergeSeries.getDataPoints().getDoubleArray(),
                       0,
                       mergeSeries.getDataPoints().size());

    mem.putInt(buffStartPosition, currSize + mergeSeries.size());
  }

  @Override
  public SimpleTimeSeries computeSimpleBuffered(WritableMemory mem, int buffStartPosition)
  {
    // create a simple time series and sort it
    Integer[] indices = new Integer[size(mem, buffStartPosition)];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = i;
    }

    Arrays.sort(indices, Comparator.comparingLong(lhs -> mem.getLong(buffStartPosition + DATA_OFFSET + lhs * Long.BYTES)));
    ImplyLongArrayList ts = new ImplyLongArrayList(indices.length);
    ImplyDoubleArrayList dp = new ImplyDoubleArrayList(indices.length);
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(ts,
                                                             dp,
                                                             getwindow(),
                                                             getStartBuffered(mem, buffStartPosition),
                                                             getEndBuffered(mem, buffStartPosition),
                                                             getMaxEntries());
    for (int idx : indices) {
      simpleTimeSeries.addDataPoint(mem.getLong(buffStartPosition + DATA_OFFSET + idx * Long.BYTES),
                                    mem.getDouble(buffStartPosition + DATA_OFFSET + (getMaxEntries() * Long.BYTES) + idx * Double.BYTES));
    }
    return simpleTimeSeries.computeSimple();
  }
}
