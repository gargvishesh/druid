/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerde;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.segment.serde.cell.StorableBuffer;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.Map;

public class SimpleTimeSeriesContainer
{
  private static final byte IS_NULL = 1;
  private static final byte[] NULL_BYTES = new byte[]{IS_NULL};

  private final boolean isBuffered;
  private final SimpleTimeSeriesBuffer simpleTimeSeriesBuffer;
  private final SimpleTimeSeries simpleTimeSeries;
  private static final Base64.Decoder DECODER = Base64.getDecoder();
  private Map<String, Object> cacheMap;

  private SimpleTimeSeriesContainer(SimpleTimeSeriesBuffer simpleTimeSeriesBuffer)
  {
    this.simpleTimeSeriesBuffer = simpleTimeSeriesBuffer;
    simpleTimeSeries = null;
    isBuffered = true;
  }

  private SimpleTimeSeriesContainer(@Nullable SimpleTimeSeries simpleTimeSeries)
  {
    this.simpleTimeSeries = simpleTimeSeries;
    simpleTimeSeriesBuffer = null;
    isBuffered = false;
  }

  public static SimpleTimeSeriesContainer createFromObject(Object object, Interval window, int maxEntries)
  {
    if (object instanceof SimpleTimeSeriesContainer) {
      return (SimpleTimeSeriesContainer) object;
    }

    if (object instanceof SimpleTimeSeries) {
      return SimpleTimeSeriesContainer.createFromInstance((SimpleTimeSeries) object);
    }

    byte[] bytes;

    if (object instanceof String) {
      bytes = DECODER.decode((String) object);
    } else {
      bytes = (byte[]) object;
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());

    return SimpleTimeSeriesContainer.createFromByteBuffer(byteBuffer, window, maxEntries);
  }

  public static SimpleTimeSeriesContainer createFromBuffer(SimpleTimeSeriesBuffer timeSeriesBuffer)
  {
    return new SimpleTimeSeriesContainer(timeSeriesBuffer);
  }

  public static SimpleTimeSeriesContainer createFromInstance(SimpleTimeSeries simpleTimeSeries)
  {
    return new SimpleTimeSeriesContainer(simpleTimeSeries);
  }

  public static SimpleTimeSeriesContainer createFromByteBuffer(ByteBuffer byteBuffer, Interval window, int maxEntries)
  {
    byte tsStatus = byteBuffer.get();

    if (tsStatus == (byte) 1) { // full empty
      return SimpleTimeSeriesContainer.createFromInstance(null);
    } else if (tsStatus == (byte) -1) { // empty series with edge points
      NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> edges = readEdgesFromBuffer(byteBuffer);
      return SimpleTimeSeriesContainer.createFromInstance(
          new SimpleTimeSeries(
              new ImplyLongArrayList(),
              new ImplyDoubleArrayList(),
              window,
              edges.lhs,
              edges.rhs,
              maxEntries,
              1L
          )
      );
    }

    // non-empty series
    if (tsStatus != 0) {
      throw DruidException.defensive("Corrupted tsStatus byte in the timeseries. The found byte is [%d]", tsStatus);
    }
    long minTimestamp = byteBuffer.getLong();
    boolean useInteger = byteBuffer.get() == 1;
    NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> edges = readEdgesFromBuffer(byteBuffer);
    SimpleTimeSeriesSerde timeSeriesSerde = SimpleTimeSeriesSerde.create(minTimestamp, useInteger);
    // we know this cannot be null because our header before this verifies non-null timeSeries
    SimpleTimeSeries rawSimpleTimeSeries = timeSeriesSerde.deserialize(byteBuffer);
    @SuppressWarnings("ConstantConditions")
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        rawSimpleTimeSeries.getTimestamps(),
        rawSimpleTimeSeries.getDataPoints(),
        window,
        edges.lhs,
        edges.rhs,
        maxEntries,
        1L
    );

    return SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries);
  }

  private static NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> readEdgesFromBuffer(ByteBuffer byteBuffer)
  {
    long edgePointStartTimestamp = byteBuffer.getLong();
    double edgePointStartData = byteBuffer.getDouble();
    long edgePointEndTimestamp = byteBuffer.getLong();
    double edgePointEndData = byteBuffer.getDouble();

    TimeSeries.EdgePoint edgePointStart = new TimeSeries.EdgePoint(edgePointStartTimestamp, edgePointStartData);
    TimeSeries.EdgePoint edgePointEnd = new TimeSeries.EdgePoint(edgePointEndTimestamp, edgePointEndData);
    return new NonnullPair<>(edgePointStart, edgePointEnd);
  }

  @JsonValue
  public byte[] getSerializedBytes()
  {
    SimpleTimeSeries timeSeries = getSimpleTimeSeries();

    if (timeSeries == null) {
      return NULL_BYTES; // null series tsStatus
    } else if (timeSeries.size() == 0) { // empty series but might have edge points
      if (timeSeries.getStart().getTimestampJson() == null && timeSeries.getEnd().getTimestampJson() == null) {
        return NULL_BYTES; // no edge points, fully emtpy series
      }
      // tsStatus + 2 * edge{long, double}
      ByteBuffer byteBuffer = ByteBuffer.allocate(Byte.BYTES + 2 * (Long.BYTES + Double.BYTES))
                                        .order(ByteOrder.nativeOrder());

      byteBuffer.put((byte) -1); // tsStatus = empty but with edge points
      writeEdgesToBuffer(byteBuffer, new NonnullPair<>(timeSeries.getStart(), timeSeries.getEnd()));
      return byteBuffer.array();
    } else { // non-empty series
      timeSeries.computeSimple();
      ImplyLongArrayList timestamps = timeSeries.getTimestamps();
      long minTimestamp = timestamps.getLong(0);
      boolean useInteger = getUseInteger(timestamps);
      SimpleTimeSeriesSerde timeSeriesSerde = SimpleTimeSeriesSerde.create(minTimestamp, useInteger);
      StorableBuffer storableBuffer = timeSeriesSerde.serializeDelayed(timeSeries);
      // tsStatus + minTimestamp + useInteger(byte) + 2 * edge{long, double} + bytes
      ByteBuffer byteBuffer = ByteBuffer.allocate(Byte.BYTES
                                                  + Long.BYTES
                                                  + Byte.BYTES
                                                  + 2 * (Long.BYTES + Double.BYTES)
                                                  + storableBuffer.getSerializedSize())
                                        .order(ByteOrder.nativeOrder());

      byteBuffer.put((byte) 0); // tsStatus = non-empty series
      byteBuffer.putLong(minTimestamp);
      byteBuffer.put((byte) (useInteger ? 1 : 0));
      writeEdgesToBuffer(byteBuffer, new NonnullPair<>(timeSeries.getStart(), timeSeries.getEnd()));
      storableBuffer.store(byteBuffer);
      return byteBuffer.array();
    }
  }

  private void writeEdgesToBuffer(ByteBuffer byteBuffer, NonnullPair<TimeSeries.EdgePoint, TimeSeries.EdgePoint> edges)
  {
    TimeSeries.EdgePoint start = edges.lhs;
    byteBuffer.putLong(start.getTimestamp());
    byteBuffer.putDouble(start.data);

    TimeSeries.EdgePoint end = edges.rhs;
    byteBuffer.putLong(end.getTimestamp());
    byteBuffer.putDouble(end.data);
  }

  @SuppressWarnings("ConstantConditions")
  public SimpleTimeSeries getSimpleTimeSeries()
  {
    return isBuffered ? simpleTimeSeriesBuffer.getSimpleTimeSeries() : simpleTimeSeries;
  }

  public Long getBucketMillis()
  {
    return getSimpleTimeSeries().getBucketMillis();
  }

  public SimpleTimeSeries computeSimple()
  {
    if (isNull()) {
      return null;
    }
    return getSimpleTimeSeries().computeSimple();
  }

  public int size()
  {
    return computeSimple().size();
  }

  public void pushInto(SimpleTimeSeries simpleTimeSeries)
  {
    Preconditions.checkNotNull(simpleTimeSeries, "merge series is null");
    maybeClearCacheMap();
    SimpleTimeSeries thisSimpleTimeSeries = getSimpleTimeSeries();
    simpleTimeSeries.addTimeSeries(thisSimpleTimeSeries);
  }

  public void pushInto(
      SimpleTimeSeriesFromByteBufferAdapter simpleByteBufferTimeSeries,
      WritableMemory writableMemory,
      int pos,
      Interval window
  )
  {
    Preconditions.checkNotNull(simpleByteBufferTimeSeries, "simple time series is null");
    Preconditions.checkNotNull(window, "window is null");
    maybeClearCacheMap();
    simpleByteBufferTimeSeries.mergeSeriesBuffered(writableMemory, pos, getSimpleTimeSeries().copyWithWindow(window));
  }

  public SimpleTimeSeriesFromByteBufferAdapter initAndPushInto(
      WritableMemory writableMemory,
      int pos,
      @Nullable Interval window,
      int maxEntries
  )
  {
    maybeClearCacheMap();
    // apply query window to properly filter data points and compute edges
    SimpleTimeSeries windowFilteredTimeSeries;
    if (window == null) {
      windowFilteredTimeSeries = getSimpleTimeSeries();
    } else {
      windowFilteredTimeSeries = getSimpleTimeSeries().copyWithWindow(window);
    }

    SimpleTimeSeriesFromByteBufferAdapter simpleByteBufferTimeSeries =
        new SimpleTimeSeriesFromByteBufferAdapter(windowFilteredTimeSeries.getWindow(), maxEntries);

    pushInto(simpleByteBufferTimeSeries, writableMemory, pos, windowFilteredTimeSeries.getWindow());

    simpleByteBufferTimeSeries.setBucketMillis(
        writableMemory,
        pos,
        getBucketMillis() == null ? -1 : getBucketMillis()
    );
    return simpleByteBufferTimeSeries;
  }

  public void pushInto(SimpleTimeSeriesContainer simpleTimeSeriesContainer)
  {
    pushInto(simpleTimeSeriesContainer.getSimpleTimeSeries());
  }

  public boolean isNull()
  {
    return isBuffered ? simpleTimeSeriesBuffer.isNull() : (simpleTimeSeries == null);
  }

  private boolean getUseInteger(ImplyLongArrayList timestamps)
  {
    return timestamps.size() == 0 || timestampRangeIsInteger(timestamps);
  }

  private boolean timestampRangeIsInteger(ImplyLongArrayList timestamps)
  {
    long minTimestamp = timestamps.getLong(0);
    long maxTimestamp = timestamps.getLong(timestamps.size() - 1);

    return maxTimestamp - minTimestamp <= Integer.MAX_VALUE;
  }

  public void addToCache(String key, Object value)
  {
    getCacheMap().put(key, value);
  }

  public Object getFromCache(String key)
  {
    return getCacheMap().get(key);
  }

  private Map<String, Object> getCacheMap()
  {
    if (cacheMap == null) {
      cacheMap = Maps.newHashMapWithExpectedSize(1);
    }
    return cacheMap;
  }

  private void maybeClearCacheMap()
  {
    if (cacheMap != null) {
      cacheMap.clear();
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleTimeSeriesContainer that = (SimpleTimeSeriesContainer) o;
    return Objects.equal(computeSimple(), that.computeSimple());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(getSimpleTimeSeries());
  }
}
