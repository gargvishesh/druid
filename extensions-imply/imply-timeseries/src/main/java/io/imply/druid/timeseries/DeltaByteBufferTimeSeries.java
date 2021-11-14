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

public class DeltaByteBufferTimeSeries extends ByteBufferTimeSeries<DeltaTimeSeries>
{
  private final DurationGranularity timeBucketGranularity;
  private final long windowStartBucket;

  public DeltaByteBufferTimeSeries(DurationGranularity timeBucketGranularity, Interval window, int maxEntries)
  {
    super(window, maxEntries);
    this.timeBucketGranularity = timeBucketGranularity;
    this.windowStartBucket = timeBucketGranularity.bucketStart(getwindow().getStartMillis());
  }

  @Override
  public void init(WritableMemory mem, int buffStartPosition)
  {
    super.init(mem, buffStartPosition);
    for (int i = 0; i < getMaxEntries(); i++) {
      // marks the minTs of the buckets to -1
      mem.putLong(buffStartPosition + DATA_OFFSET + i * 2 * Long.BYTES, -1);
    }
  }

  private void addDeltaSeriesEntry(
      WritableMemory mem,
      int buffStartPosition,
      long minTs,
      double minTsData,
      long maxTs,
      double maxTsData
  )
  {
    long bucketStart = timeBucketGranularity.bucketStart(minTs);
    int slotId = (int) ((bucketStart - windowStartBucket) / getTimeBucketGranularity().getDurationMillis());
    int tsOffset = buffStartPosition + DATA_OFFSET + slotId * 2 * Long.BYTES;
    int dataPointOffset = buffStartPosition + DATA_OFFSET + (2 * getMaxEntries() * Long.BYTES) + slotId * 2 * Double.BYTES;
    long[] tsVals = new long[2];
    mem.getLongArray(tsOffset, tsVals, 0, tsVals.length);
    if (tsVals[0] == -1) {
      mem.putLong(tsOffset, minTs);
      mem.putLong(tsOffset + Long.BYTES, maxTs);
      mem.putDouble(dataPointOffset, minTsData);
      mem.putDouble(dataPointOffset + Double.BYTES, maxTsData);
      mem.putInt(buffStartPosition, size(mem, buffStartPosition) + 1);
    } else {
      if (minTs < tsVals[0]) {
        mem.putLong(tsOffset, minTs);
        mem.putDouble(dataPointOffset, minTsData);
      }
      if (maxTs > tsVals[1]) {
        mem.putLong(tsOffset + Long.BYTES, maxTs);
        mem.putDouble(dataPointOffset + Double.BYTES, maxTsData);
      }
    }
  }

  @Override
  void internalAddDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data)
  {
    addDeltaSeriesEntry(
        mem,
        buffStartPosition,
        timestamp,
        data,
        timestamp,
        data);
  }

  @Override
  void internalMergeSeriesBuffered(WritableMemory mem, int buffStartPosition, DeltaTimeSeries mergeSeries)
  {
    ImplyLongArrayList timestamps = mergeSeries.getTimestamps();
    ImplyDoubleArrayList dataPoints = mergeSeries.getDataPoints();
    for (int i = 0; i < mergeSeries.size(); i++) {
      addDeltaSeriesEntry(mem,
                          buffStartPosition,
                          timestamps.getLong(2 * i),
                          dataPoints.getDouble(2 * i),
                          timestamps.getLong(2 * i + 1),
                          dataPoints.getDouble(2 * i + 1));
    }
  }

  public DurationGranularity getTimeBucketGranularity()
  {
    return timeBucketGranularity;
  }

  public DeltaTimeSeries computeDeltaBuffered(WritableMemory mem, int buffStartPosition)
  {
    int currSize = size(mem, buffStartPosition);
    int tsOffset = buffStartPosition + DATA_OFFSET;
    int dataPointOffset = tsOffset + (2 * getMaxEntries() * Long.BYTES);
    ImplyLongArrayList timestamps = new ImplyLongArrayList(2 * currSize);
    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList(2 * currSize);
    for (int i = 0; i < getMaxEntries(); i++) {
      int currOffset = i * 2 * Long.BYTES;
      long[] tsVals = new long[2];
      mem.getLongArray(tsOffset + currOffset, tsVals, 0, tsVals.length);
      if (tsVals[0] != -1) {
        double[] dp = new double[2];
        mem.getDoubleArray(dataPointOffset + currOffset, dp, 0, dp.length);

        timestamps.add(tsVals[0]);
        dataPoints.add(dp[0]);
        timestamps.add(tsVals[1]);
        dataPoints.add(dp[1]);
      }
    }
    return new DeltaTimeSeries(timestamps,
                              dataPoints,
                              getTimeBucketGranularity(),
                              getwindow(),
                              getStartBuffered(mem, buffStartPosition),
                              getEndBuffered(mem, buffStartPosition),
                              getMaxEntries());
  }

  @Override
  public SimpleTimeSeries computeSimpleBuffered(WritableMemory mem, int buffStartPosition)
  {
    return computeDeltaBuffered(mem, buffStartPosition).computeSimple();
  }
}
