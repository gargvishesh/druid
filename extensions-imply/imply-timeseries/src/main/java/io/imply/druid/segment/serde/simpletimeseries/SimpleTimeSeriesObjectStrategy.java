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

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

    int numElements = buffer.getInt();
    ImplyLongArrayList timestamps = new ImplyLongArrayList(numElements);

    for (int i = 0; i < numElements; i++) {
      timestamps.add(buffer.getLong());
    }

    ImplyDoubleArrayList dataPoints = new ImplyDoubleArrayList(numElements);

    for (int i = 0; i < numElements; i++) {
      dataPoints.add(buffer.getDouble());
    }

    return new SimpleTimeSeries(
        timestamps,
        dataPoints,
        SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW,
        numElements
    );
  }

  @Override
  @NotNull
  public byte[] toBytes(@Nullable SimpleTimeSeries val)
  {
    if (val == null) {
      return NULL_TIME_SERIES;
    }
    ImplyLongArrayList timestamps = val.getTimestamps();
    ImplyDoubleArrayList dataPoints = val.getDataPoints();
    cosort(timestamps, dataPoints);

    int sizeBytes = (Integer.BYTES
                     + timestamps.size() * Long.BYTES
                     + dataPoints.size() * Double.BYTES
                     + 2 * Long.BYTES);
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

  private static void cosort(ImplyLongArrayList timestamps, ImplyDoubleArrayList dataPoints)
  {
    List<TimestampDataPointPair> sortedPairList = new ArrayList<>();

    for (int i = 0; i < timestamps.size(); i++) {
      sortedPairList.add(new TimestampDataPointPair(timestamps.getLong(i), dataPoints.getDouble(i)));
    }

    sortedPairList.sort(Comparator.comparingLong(a -> a.timestamp));

    timestamps.clear();
    dataPoints.clear();

    sortedPairList.forEach(p -> {
      timestamps.add(p.timestamp);
      dataPoints.add(p.dataPoint);
    });
  }

  private static class TimestampDataPointPair
  {
    private final long timestamp;
    private final double dataPoint;

    public TimestampDataPointPair(long timestamp, double dataPoint)
    {
      this.timestamp = timestamp;
      this.dataPoint = dataPoint;
    }
  }
}
