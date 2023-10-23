/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

public class DownsampledSumTimeSeriesFromByteBufferAdapter extends TimeSeriesFromByteBufferAdapter<SimpleTimeSeries>
{
  private final DurationGranularity timeBucketGranularity;
  private final long windowStartBucket;
  private final byte[] bitsetInitState;

  public DownsampledSumTimeSeriesFromByteBufferAdapter(DurationGranularity timeBucketGranularity, Interval window, int maxEntries)
  {
    super(window, maxEntries);
    this.timeBucketGranularity = timeBucketGranularity;
    this.windowStartBucket = timeBucketGranularity.bucketStart(getWindow().getStartMillis());
    this.bitsetInitState = new byte[DownsampledSumTimeSeriesAggregatorFactory.getTimeseriesBitmapBytesSize(getMaxEntries())];
  }

  @Override
  public void init(WritableMemory mem, int buffStartPosition)
  {
    super.init(mem, buffStartPosition);
    mem.putByteArray(getBitmapStartByte(buffStartPosition), bitsetInitState, 0, bitsetInitState.length);
  }

  private void addDownsampledSumSeriesEntry(WritableMemory mem, int buffStartPosition, long timestamp, double sumPoint)
  {
    long bucketStart = getTimeBucketGranularity().bucketStart(timestamp);
    int slotId = (int) ((bucketStart - windowStartBucket) / getTimeBucketGranularity().getDurationMillis());
    int sumOffset = buffStartPosition + DATA_OFFSET;
    int relOffset = slotId * Double.BYTES;
    double prevSum = 0;
    if (!isBucketInitialized(mem, buffStartPosition, slotId)) {
      initializeBucket(mem, buffStartPosition, slotId);
      mem.putInt(buffStartPosition, size(mem, buffStartPosition) + 1);
    } else {
      prevSum = mem.getDouble(sumOffset + relOffset);
    }
    mem.putDouble(sumOffset + relOffset, prevSum + sumPoint);
  }

  @Override
  void internalAddDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data)
  {
    addDownsampledSumSeriesEntry(mem, buffStartPosition, timestamp, data);
  }

  @Override
  void internalMergeSeriesBuffered(WritableMemory mem, int buffStartPosition, SimpleTimeSeries mergeSeries)
  {
    ImplyLongArrayList timestamps = mergeSeries.getTimestamps();
    ImplyDoubleArrayList sumPoints = mergeSeries.getDataPoints();
    for (int i = 0; i < mergeSeries.size(); i++) {
      addDownsampledSumSeriesEntry(mem, buffStartPosition, timestamps.getLong(i), sumPoints.getDouble(i));
    }
  }

  public DurationGranularity getTimeBucketGranularity()
  {
    return timeBucketGranularity;
  }

  public DownsampledSumTimeSeries computeDownsampledSumBuffered(WritableMemory mem, int buffStartPosition)
  {
    int currSize = size(mem, buffStartPosition);
    int sumOffset = buffStartPosition + DATA_OFFSET;
    ImplyLongArrayList timestamps = new ImplyLongArrayList(currSize);
    ImplyDoubleArrayList sumPoints = new ImplyDoubleArrayList(currSize);
    for (int i = 0; i < getMaxEntries(); i++) {
      if (!isBucketInitialized(mem, buffStartPosition, i)) {
        continue;
      }
      int relOffset = i * Double.BYTES;
      double sumPoint = mem.getDouble(sumOffset + relOffset);
      timestamps.add(windowStartBucket + i * timeBucketGranularity.getDurationMillis());
      sumPoints.add(sumPoint);
    }
    return new DownsampledSumTimeSeries(
        timestamps,
        sumPoints,
        getTimeBucketGranularity(),
        getWindow(),
        getStartBuffered(mem, buffStartPosition),
        getEndBuffered(mem, buffStartPosition),
        getMaxEntries()
    );
  }

  @Override
  public SimpleTimeSeries computeSimpleBuffered(WritableMemory mem, int buffStartPosition)
  {
    return computeDownsampledSumBuffered(mem, buffStartPosition).computeSimple();
  }

  private boolean isBucketInitialized(WritableMemory mem, int position, int bucketId)
  {
    int startByte = getBitmapStartByte(position);
    int targetByte = startByte + bucketId / Byte.SIZE;
    int targetBit = bucketId % Byte.SIZE;
    return (mem.getByte(targetByte) & (1 << targetBit)) != 0;
  }

  private void initializeBucket(WritableMemory mem, int position, int bucketId)
  {
    if (isBucketInitialized(mem, position, bucketId)) {
      return;
    }
    int startByte = getBitmapStartByte(position);
    int targetByte = startByte + bucketId / Byte.SIZE;
    int targetBit = bucketId % Byte.SIZE;
    byte newValue = (byte) (mem.getByte(targetByte) | ((byte) 1 << targetBit));
    mem.putByte(targetByte, newValue);
  }

  private int getBitmapStartByte(int position)
  {
    return position + DATA_OFFSET + getMaxEntries() * Double.BYTES;
  }
}
