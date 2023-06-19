/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.google.common.base.Preconditions;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.UOE;
import org.joda.time.Interval;

public abstract class ByteBufferTimeSeries<T extends TimeSeries<T>>
{
  // 4 bytes TS size + 8 bytes bucket millis + 2 X (edge timestamp + edge data point)
  public static final int DATA_OFFSET = Integer.BYTES + Long.BYTES + 2 * (Long.BYTES + Double.BYTES);
  public static final int BUCKET_MILLIS_OFFSET = Integer.BYTES;
  protected static final int START_TS_OFFSET = BUCKET_MILLIS_OFFSET + Long.BYTES;
  protected static final int END_TS_OFFSET = START_TS_OFFSET + Long.BYTES + Double.BYTES;
  private final int maxEntries;
  private final Interval window;

  public ByteBufferTimeSeries(Interval window, int maxEntries)
  {
    this.window = Preconditions.checkNotNull(window, "window is null");
    this.maxEntries = maxEntries;
  }

  public void init(WritableMemory mem, int buffStartPosition)
  {
    mem.putInt(buffStartPosition, -1);
    mem.putLong(buffStartPosition + BUCKET_MILLIS_OFFSET, 1);
    mem.putLong(buffStartPosition + START_TS_OFFSET, -1);
    mem.putDouble(buffStartPosition + START_TS_OFFSET + Long.BYTES, -1);
    mem.putLong(buffStartPosition + END_TS_OFFSET, -1);
    mem.putDouble(buffStartPosition + END_TS_OFFSET + Long.BYTES, -1);
  }

  public Interval getWindow()
  {
    return window;
  }

  public int getMaxEntries()
  {
    return maxEntries;
  }


  public void addDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data)
  {
    if (isNull(mem, buffStartPosition)) {
      mem.putInt(buffStartPosition, 0);
    }
    Interval window = getWindow();
    // if the timestamp is in visible window (if any), then add it. Otherwise update the edges if needed
    if (window.contains(timestamp)) {
      internalAddDataPointBuffered(mem, buffStartPosition, timestamp, data);
    } else {
      if (timestamp < window.getStartMillis()) {
        long ts = mem.getLong(buffStartPosition + START_TS_OFFSET);
        if (ts < timestamp) {
          mem.putLong(buffStartPosition + START_TS_OFFSET, timestamp);
          mem.putDouble(buffStartPosition + START_TS_OFFSET + Long.BYTES, data);
        }
      } else if (timestamp >= window.getEndMillis()) {
        long ts = mem.getLong(buffStartPosition + END_TS_OFFSET);
        if (ts > timestamp) {
          mem.putLong(buffStartPosition + END_TS_OFFSET, timestamp);
          mem.putDouble(buffStartPosition + END_TS_OFFSET + Long.BYTES, data);
        }
      }
    }
  }

  abstract void internalAddDataPointBuffered(WritableMemory mem, int buffStartPosition, long timestamp, double data);

  public void mergeSeriesBuffered(WritableMemory mem, int buffStartPosition, T mergeSeries)
  {
    // can't merge series with different visible windows as of now
    boolean compatibleMerge = (mergeSeries.getWindow() == null && getWindow() == null) ||
                              mergeSeries.getWindow().equals(getWindow());
    if (!compatibleMerge) {
      throw new UOE("The time series to merge have different visible windows : (%s, %s)", getWindow(), mergeSeries.getWindow());
    }

    // update the edges
    long currStartTs = mem.getLong(buffStartPosition + START_TS_OFFSET);
    long maxStartTs = Math.max(mergeSeries.getStart().getTimestamp(), currStartTs);

    long currEndTs = mem.getLong(buffStartPosition + END_TS_OFFSET);
    long minEndTs;
    if (currEndTs == -1) {
      minEndTs = mergeSeries.getEnd().getTimestamp();
    } else if (mergeSeries.getEnd().getTimestamp() == -1) {
      minEndTs = currEndTs;
    } else {
      minEndTs = Math.min(currEndTs, mergeSeries.getEnd().getTimestamp());
    }

    if (maxStartTs != currStartTs) {
      setStartBuffered(mem, buffStartPosition, mergeSeries.getStart());
    }
    if (minEndTs != currEndTs) {
      setEndBuffered(mem, buffStartPosition, mergeSeries.getEnd());
    }

    // if null, set the timeseries to an empty one for merge
    if (isNull(mem, buffStartPosition)) {
      mem.putInt(buffStartPosition, 0);
    }
    // merge the visible window
    internalMergeSeriesBuffered(mem, buffStartPosition, mergeSeries);
  }

  abstract void internalMergeSeriesBuffered(WritableMemory mem, int buffStartPosition, T mergeSeries);

  public abstract SimpleTimeSeries computeSimpleBuffered(WritableMemory mem, int buffStartPosition);

  public TimeSeries.EdgePoint getStartBuffered(WritableMemory mem, int buffStartPosition)
  {
    return new TimeSeries.EdgePoint(mem.getLong(buffStartPosition + START_TS_OFFSET),
                                    mem.getDouble(buffStartPosition + START_TS_OFFSET + Long.BYTES));
  }

  public void setStartBuffered(WritableMemory mem, int buffStartPosition, TimeSeries.EdgePoint newStart)
  {
    mem.putLong(buffStartPosition + START_TS_OFFSET, newStart.getTimestamp());
    mem.putDouble(buffStartPosition + START_TS_OFFSET + Long.BYTES, newStart.getData());
  }

  public TimeSeries.EdgePoint getEndBuffered(WritableMemory mem, int buffStartPosition)
  {
    return new TimeSeries.EdgePoint(mem.getLong(buffStartPosition + END_TS_OFFSET),
                                    mem.getDouble(buffStartPosition + END_TS_OFFSET + Long.BYTES));
  }

  public void setEndBuffered(WritableMemory mem, int buffStartPosition, TimeSeries.EdgePoint newEnd)
  {
    mem.putLong(buffStartPosition + END_TS_OFFSET, newEnd.getTimestamp());
    mem.putDouble(buffStartPosition + END_TS_OFFSET + Long.BYTES, newEnd.getData());
  }

  public boolean isNull(WritableMemory mem, int buffStartPosition)
  {
    return size(mem, buffStartPosition) < 0;
  }

  public int size(WritableMemory mem, int buffStartPosition)
  {
    return mem.getInt(buffStartPosition);
  }

  public long getBucketMillis(WritableMemory mem, int buffStartPosition)
  {
    return mem.getLong(buffStartPosition + BUCKET_MILLIS_OFFSET);
  }

  public void setBucketMillis(WritableMemory mem, int buffStartPosition, long newValue)
  {
    mem.putLong(buffStartPosition + BUCKET_MILLIS_OFFSET, newValue);
  }
}
