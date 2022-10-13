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
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesSimpleStagedSerde implements StagedSerde<SimpleTimeSeries>
{
  @Override
  public StorableBuffer serializeDelayed(@Nullable SimpleTimeSeries value)
  {
    if (value == null || value.size() == 0) {
      return StorableBuffer.EMPTY;
    }
    // force flattening of recursive structures
    value.computeSimple();
    ImplyLongArrayList timestamps = value.getTimestamps();
    ImplyDoubleArrayList dataPoints = value.getDataPoints();

    int sizeBytes = (Integer.BYTES + timestamps.size() * Long.BYTES + dataPoints.size() * Double.BYTES);

    return new StorableBuffer()
    {
      @Override
      public void store(ByteBuffer byteBuffer)
      {
        byteBuffer.putInt(timestamps.size());
        timestamps.forEach(byteBuffer::putLong);
        dataPoints.forEach(byteBuffer::putDouble);
      }

      @Override
      public int getSerializedSize()
      {
        return sizeBytes;
      }
    };
  }

  @Nullable
  @Override
  public SimpleTimeSeries deserialize(ByteBuffer byteBuffer)
  {
    if (byteBuffer.remaining() == 0) {
      return null;
    }

    int count = byteBuffer.getInt();
    long[] timestampValues = new long[count];
    byteBuffer.asLongBuffer().get(timestampValues);
    byteBuffer.position(byteBuffer.position() + Long.BYTES * count);

    ImplyLongArrayList timestamps = new ImplyLongArrayList(timestampValues);

    double[] doubleValues = new double[count];
    byteBuffer.asDoubleBuffer().get(doubleValues);
    byteBuffer.position(byteBuffer.position() + Double.BYTES * count);

    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList(doubleValues);

    return new SimpleTimeSeries(timestamps, dataPoints, SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, count);

  }
}
