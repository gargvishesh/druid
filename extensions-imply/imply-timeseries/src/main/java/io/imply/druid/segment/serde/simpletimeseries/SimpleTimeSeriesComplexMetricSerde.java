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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesComplexMetricSerde extends ComplexMetricSerde
{
  public static final String TYPE_NAME = "imply-ts-simple";
  public static final Interval ALL_TIME_WINDOW = Intervals.utc(Long.MIN_VALUE, Long.MAX_VALUE);

  static final SimpleTimeSeriesObjectStrategy SIMPLE_TIME_SERIES_BASIC_SERDE =
      new SimpleTimeSeriesObjectStrategy();

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
      public Class<? extends SimpleTimeSeries> extractedClass()
      {
        return SimpleTimeSeries.class;
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
  public GenericColumnSerializer<SimpleTimeSeries> getSerializer(
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
        new SimpleTimeSeriesComplexColumn.Factory(buffer, NativeClearedByteBufferProvider.DEFAULT);

    builder.setComplexColumnSupplier(complexColumnFactory::create);
  }

  @Override
  public ObjectStrategy<SimpleTimeSeries> getObjectStrategy()
  {
    return SIMPLE_TIME_SERIES_BASIC_SERDE;
  }
}
