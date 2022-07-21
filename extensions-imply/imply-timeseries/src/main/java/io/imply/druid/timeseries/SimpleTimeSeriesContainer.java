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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerde;
import io.imply.druid.segment.serde.simpletimeseries.StorableBuffer;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.datasketches.memory.WritableMemory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleTimeSeriesContainer
{
  private static final byte IS_NULL = 1;
  private static final byte[] NULL_BYTES = new byte[]{IS_NULL};

  private final boolean isBuffered;
  private final SimpleTimeSeriesBuffer simpleTimeSeriesBuffer;
  private final SimpleTimeSeries simpleTimeSeries;

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
        maxEntries
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
      ByteBuffer byteBuffer =
          ByteBuffer.allocate(Byte.BYTES
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

  public SimpleTimeSeries computeSimple()
  {
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
    }
  }

  public void pushInto(
      SimpleByteBufferTimeSeries simpleByteBufferTimeSeries,
      WritableMemory writableMemory,
      int pos,
      Interval window
  )
  {
    // apply query window to properly filter data points and compute edges
    SimpleTimeSeries windowFilteredTimeSeries = getSimpleTimeSeries().withWindow(window);

    simpleByteBufferTimeSeries.mergeSeriesBuffered(writableMemory, pos, windowFilteredTimeSeries);
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
    return Objects.equal(getSimpleTimeSeries(), that.getSimpleTimeSeries());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(getSimpleTimeSeries());
  }
}
