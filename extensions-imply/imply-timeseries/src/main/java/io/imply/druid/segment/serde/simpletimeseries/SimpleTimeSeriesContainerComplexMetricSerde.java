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
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SimpleTimeSeriesContainerComplexMetricSerde extends ComplexMetricSerde
{
  public static final String TYPE_NAME =
      Objects.requireNonNull(BaseTimeSeriesAggregatorFactory.TYPE.getComplexTypeName());

  static final SimpleTimeSeriesContainerObjectStrategy SIMPLE_TIME_SERIES_OBJECT_STRATEGY =
      new SimpleTimeSeriesContainerObjectStrategy();

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor<Object> getExtractor()
  {
    return new ComplexMetricExtractor<Object>()
    {
      @Override
      public Class<? extends SimpleTimeSeriesContainer> extractedClass()
      {
        return SimpleTimeSeriesContainer.class;
      }

      @Nullable
      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        return inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public GenericColumnSerializer<SimpleTimeSeriesContainer> getSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String column
  )
  {
    return new SimpleTimeSeriesColumnSerializer(segmentWriteOutMedium);
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    SimpleTimeSeriesComplexColumn.Factory complexColumnFactory =
        new SimpleTimeSeriesComplexColumn.Factory(buffer);

    builder.setComplexColumnSupplier(complexColumnFactory::create);
  }

  @Override
  public ObjectStrategy<SimpleTimeSeriesContainer> getObjectStrategy()
  {
    return SIMPLE_TIME_SERIES_OBJECT_STRATEGY;
  }

  @Override
  public byte[] toBytes(@Nullable Object val)
  {
    if (val == null) {
      return new byte[0];
    }

    if (val instanceof SimpleTimeSeriesContainer) {
      if (((SimpleTimeSeriesContainer) val).isNull()) {
        return new byte[0];
      }
      SimpleTimeSeriesContainer container = (SimpleTimeSeriesContainer) val;
      SimpleTimeSeries simpleTimeSeries = container.getSimpleTimeSeries();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      try {
        dos.writeInt(simpleTimeSeries.getMaxEntries());
        String intervalString = simpleTimeSeries.getWindow().toString();
        dos.writeUTF(intervalString);
        dos.write(container.getSerializedBytes());
        dos.flush();
        return baos.toByteArray();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw DruidException.defensive(
        "Found unknown type. Expected [%s] but found [%s]",
        SimpleTimeSeriesContainer.class,
        val.getClass()
    );
  }

  @Override
  public Object fromBytes(byte[] data, int start, int numBytes)
  {
    if (data == null || numBytes == 0) {
      return SimpleTimeSeriesContainer.createFromInstance(null);
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(data, start, numBytes);
    DataInputStream dis = new DataInputStream(bais);
    try {
      int maxEntries = dis.readInt();
      String intervalString = dis.readUTF();
      byte[] tsBytes = new byte[dis.available()];
      int bytesRead = dis.read(tsBytes);
      if (bytesRead != tsBytes.length) {
        throw DruidException.defensive(
            "Invalid read operation. Expected [%d] bytes, but read [%d] bytes",
            tsBytes.length,
            bytesRead
        );
      }
      return SimpleTimeSeriesContainer.createFromObject(
          tsBytes,
          Intervals.of(intervalString),
          maxEntries
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
