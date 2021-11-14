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
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.joda.time.Interval;

public class MeanByteBufferTimeSeries extends ByteBufferTimeSeries<MeanTimeSeries>
{
  private final DurationGranularity timeBucketGranularity;
  private final long windowStartBucket;
  private final int countRelOffset;

  public MeanByteBufferTimeSeries(DurationGranularity timeBucketGranularity, Interval window, int maxEntries)
  {
    super(window, maxEntries);
    this.timeBucketGranularity = timeBucketGranularity;
    this.windowStartBucket = timeBucketGranularity.bucketStart(getwindow().getStartMillis());
    this.countRelOffset = getMaxEntries() * Double.BYTES;
  }

  @Override
  public void init(WritableMemory mem, int buffStartPosition)
  {
    super.init(mem, buffStartPosition);
    for (int i = 0; i < getMaxEntries(); i++) {
      // marks the countPoints of the buckets to -1
      mem.putLong(buffStartPosition + DATA_OFFSET + countRelOffset + i * Long.BYTES, -1);
    }
  }

  private void addMeanSeriesEntry(WritableMemory mem, int buffStartPosition, long timestamp, double sumPoint, long countPoint)
  {
    long bucketStart = getTimeBucketGranularity().bucketStart(timestamp);
    int currSize = size(mem, buffStartPosition);
    int slotId = (int) ((bucketStart - windowStartBucket) / getTimeBucketGranularity().getDurationMillis());
    int sumOffset = buffStartPosition + DATA_OFFSET;
    int countOffset = sumOffset + countRelOffset;
    int relOffset = slotId * Long.BYTES;
    double prevSum = 0;
    long prevCount = 0;
    if (mem.getLong(countOffset + relOffset) == -1) {
      mem.putInt(buffStartPosition, currSize + 1);
    } else {
      prevSum = mem.getDouble(sumOffset + relOffset);
      prevCount = mem.getLong(countOffset + relOffset);
    }
    mem.putDouble(sumOffset + relOffset, prevSum + sumPoint);
    mem.putLong(countOffset + relOffset, prevCount + countPoint);
  }

  @Override
  void internalAddDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data)
  {
    addMeanSeriesEntry(mem, buffStartPosition, timestamp, data, 1);
  }

  @Override
  void internalMergeSeriesBuffered(WritableMemory mem, int buffStartPosition, MeanTimeSeries mergeSeries)
  {
    ImplyLongArrayList timestamps = mergeSeries.getBucketStarts();
    ImplyDoubleArrayList sumPoints = mergeSeries.getSumPoints();
    ImplyLongArrayList countPoints = mergeSeries.getCountPoints();
    for (int i = 0; i < mergeSeries.size(); i++) {
      addMeanSeriesEntry(mem, buffStartPosition, timestamps.getLong(i), sumPoints.getDouble(i), countPoints.getLong(i));
    }
  }

  public DurationGranularity getTimeBucketGranularity()
  {
    return timeBucketGranularity;
  }

  public MeanTimeSeries computeMeanBuffered(WritableMemory mem, int buffStartPosition)
  {
    int currSize = size(mem, buffStartPosition);
    int sumOffset = buffStartPosition + DATA_OFFSET;
    int countOffset = sumOffset + countRelOffset;
    ImplyLongArrayList timestamps = new ImplyLongArrayList(currSize);
    ImplyDoubleArrayList sumPoints = new ImplyDoubleArrayList(currSize);
    ImplyLongArrayList countPoints = new ImplyLongArrayList(currSize);
    for (int i = 0; i < getMaxEntries(); i++) {
      int relOffset = i * Long.BYTES;
      long countPoint = mem.getLong(countOffset + relOffset);
      if (countPoint != -1) {
        double sumPoint = mem.getDouble(sumOffset + relOffset);
        timestamps.add(windowStartBucket + i * timeBucketGranularity.getDurationMillis());
        sumPoints.add(sumPoint);
        countPoints.add(countPoint);
      }
    }
    return new MeanTimeSeries(timestamps,
                              sumPoints,
                              countPoints,
                              getTimeBucketGranularity(),
                              getwindow(),
                              getStartBuffered(mem, buffStartPosition),
                              getEndBuffered(mem, buffStartPosition),
                              getMaxEntries());
  }

  @Override
  public SimpleTimeSeries computeSimpleBuffered(WritableMemory mem, int buffStartPosition)
  {
    return computeMeanBuffered(mem, buffStartPosition).computeSimple();
  }
}
