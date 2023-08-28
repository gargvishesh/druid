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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerde;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.serde.cell.StorableBuffer;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

public class SimpleTimeSeriesContainer
{
  private static final byte IS_NULL = 1;
  private static final byte[] NULL_BYTES = new byte[]{IS_NULL};

  private final boolean isBuffered;
  private final SimpleTimeSeriesBuffer simpleTimeSeriesBuffer;
  private final SimpleTimeSeries simpleTimeSeries;
  private static final Base64.Decoder DECODER = Base64.getDecoder();

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
    boolean isNull = byteBuffer.get() == 1;

    if (isNull) {
      return SimpleTimeSeriesContainer.createFromInstance(null);
    }

    long minTimestamp = byteBuffer.getLong();
    boolean useInteger = byteBuffer.get() == 1;
    long edgePointStartTimestamp = byteBuffer.getLong();
    double edgePointStartData = byteBuffer.getDouble();
    long edgePointEndTimestamp = byteBuffer.getLong();
    double edgePointEndData = byteBuffer.getDouble();

    TimeSeries.EdgePoint edgePointStart = new TimeSeries.EdgePoint(edgePointStartTimestamp, edgePointStartData);
    TimeSeries.EdgePoint edgePointEnd = new TimeSeries.EdgePoint(edgePointEndTimestamp, edgePointEndData);
    SimpleTimeSeriesSerde timeSeriesSerde = SimpleTimeSeriesSerde.create(minTimestamp, useInteger);
    // we know this cannot be null because our header before this verifies non-null timeSeries
    SimpleTimeSeries rawSimpleTimeSeries = timeSeriesSerde.deserialize(byteBuffer);
    @SuppressWarnings("ConstantConditions")
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        rawSimpleTimeSeries.getTimestamps(),
        rawSimpleTimeSeries.getDataPoints(),
        window,
        edgePointStart,
        edgePointEnd,
        maxEntries,
        1L
    );

    return SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries);
  }

  @JsonValue
  public byte[] getSerializedBytes()
  {
    SimpleTimeSeries timeSeries = getSimpleTimeSeries();

    if (timeSeries == null || timeSeries.size() == 0) {
      return NULL_BYTES;
    } else {
      timeSeries.computeSimple();
      ImplyLongArrayList timestamps = timeSeries.getTimestamps();
      long minTimestamp = timestamps.getLong(0);
      boolean useInteger = getUseInteger(timestamps);
      SimpleTimeSeriesSerde timeSeriesSerde = SimpleTimeSeriesSerde.create(minTimestamp, useInteger);
      StorableBuffer storableBuffer = timeSeriesSerde.serializeDelayed(timeSeries);
      // isNull + minTimestamp + useInteger(byte) + 2 * edge{long, double} + bytes
      ByteBuffer byteBuffer = ByteBuffer.allocate(Byte.BYTES
                                                  + Long.BYTES
                                                  + Byte.BYTES
                                                  + 2 * (Long.BYTES + Double.BYTES)
                                                  + storableBuffer.getSerializedSize())
                                        .order(ByteOrder.nativeOrder());

      byteBuffer.put((byte) 0); // isNull = false
      byteBuffer.putLong(minTimestamp);
      byteBuffer.put((byte) (useInteger ? 1 : 0));

      TimeSeries.EdgePoint start = timeSeries.getStart();

      byteBuffer.putLong(start.getTimestamp());
      byteBuffer.putDouble(start.data);

      TimeSeries.EdgePoint end = timeSeries.getEnd();

      byteBuffer.putLong(end.getTimestamp());
      byteBuffer.putDouble(end.data);
      storableBuffer.store(byteBuffer);

      return byteBuffer.array();
    }
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

  public void pushInto(SimpleTimeSeries simpleTimeSeries)
  {
    SimpleTimeSeries thisSimpleTimeSeries = getSimpleTimeSeries();
    if (thisSimpleTimeSeries != null) {
      ImplyLongArrayList timestamps = thisSimpleTimeSeries.getTimestamps();
      ImplyDoubleArrayList datapoints = thisSimpleTimeSeries.getDataPoints();

      for (int i = 0; i < timestamps.size(); i++) {
        simpleTimeSeries.addDataPoint(timestamps.getLong(i), datapoints.getDouble(i));
      }

      final TimeSeries.EdgePoint startBoundary = thisSimpleTimeSeries.getStart();
      if (startBoundary != null && startBoundary.getTimestampJson() != null) {
        simpleTimeSeries.addDataPoint(startBoundary.getTimestamp(), startBoundary.getData());
      }

      final TimeSeries.EdgePoint endBoundary = thisSimpleTimeSeries.getEnd();
      if (endBoundary != null && endBoundary.getTimestampJson() != null) {
        simpleTimeSeries.addDataPoint(endBoundary.getTimestamp(), endBoundary.getData());
      }
    }
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
    simpleByteBufferTimeSeries.mergeSeriesBuffered(writableMemory, pos, getSimpleTimeSeries().copyWithWindow(window));
  }

  public SimpleTimeSeriesFromByteBufferAdapter initAndPushInto(
      WritableMemory writableMemory,
      int pos,
      @Nullable Interval window,
      int maxEntries
  )
  {
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
