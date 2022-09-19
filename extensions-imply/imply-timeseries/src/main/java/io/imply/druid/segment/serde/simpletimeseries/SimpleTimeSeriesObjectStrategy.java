/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleTimeSeriesObjectStrategy implements ObjectStrategy<SimpleTimeSeries>
{
  private static final byte[] NULL_TIME_SERIES = new byte[0];

  @Override
  public Class<? extends SimpleTimeSeries> getClazz()
  {
    return SimpleTimeSeries.class;
  }

  @Override
  public SimpleTimeSeries fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return null;
    }

    int count = buffer.getInt();
    long[] timestampValues = new long[count];
    buffer.asLongBuffer().get(timestampValues);
    buffer.position(buffer.position() + Long.BYTES * count);

    ImplyLongArrayList timestamps = new ImplyLongArrayList(timestampValues);

    double[] doubleValues = new double[count];
    buffer.asDoubleBuffer().get(doubleValues);
    buffer.position(buffer.position() + Double.BYTES * count);

    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList(doubleValues);

    return new SimpleTimeSeries(timestamps, dataPoints, SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, count);
  }

  @Override
  @Nonnull
  public byte[] toBytes(@Nullable SimpleTimeSeries val)
  {
    if (val == null || val.size() == 0) {
      return NULL_TIME_SERIES;
    }
    // force flattening of recursive structures
    val.computeSimple();
    ImplyLongArrayList timestamps = val.getTimestamps();
    ImplyDoubleArrayList dataPoints = val.getDataPoints();

    int sizeBytes = (Integer.BYTES + timestamps.size() * Long.BYTES + dataPoints.size() * Double.BYTES);
    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeBytes).order(ByteOrder.nativeOrder());

    byteBuffer.putInt(timestamps.size());
    timestamps.forEach(byteBuffer::putLong);
    dataPoints.forEach(byteBuffer::putDouble);

    return byteBuffer.array();
  }

  @Override
  public int compare(SimpleTimeSeries o1, SimpleTimeSeries o2)
  {
    int len = Math.min(o1.size(), o2.size());
    ImplyLongArrayList timestamps1 = o1.getTimestamps();
    ImplyLongArrayList timestamps2 = o2.getTimestamps();

    for (int i = 0; i < len; i++) {
      int result = Long.compare(timestamps1.getLong(i), timestamps2.getLong(i));
      if (result != 0) {
        return result;
      }
    }

    return Integer.compare(o1.size(), o2.size());
  }
}
