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
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesView
{
  static final ByteBuffer NULL_ROW = ByteBuffer.wrap(new byte[0]);

  private final RowReader rowReader;
  private final ObjectStrategy<SimpleTimeSeries> objectStrategy;

  public SimpleTimeSeriesView(RowReader rowReader, SimpleTimeSeriesObjectStrategy objectStrategy)
  {
    this.rowReader = rowReader;
    this.objectStrategy = objectStrategy;
  }

  public static SimpleTimeSeriesView create(
      ByteBuffer originalByteBuffer,
      ByteBuffer rowIndexUncompressedBlock,
      ByteBuffer dataUncompressedBlock
  )
  {
    Factory factory = new Factory(originalByteBuffer);

    return factory.create(rowIndexUncompressedBlock, dataUncompressedBlock);
  }

  @Nullable
  public SimpleTimeSeries getRow(int rowNumber)
  {
    ByteBuffer payload = rowReader.getRow(rowNumber);

    return objectStrategy.fromByteBuffer(payload, payload.limit());
  }

  public static class Factory
  {
    private final RowReader.Builder rowReaderBuilder;

    public Factory(ByteBuffer originalByteBuffer)
    {
      rowReaderBuilder = new RowReader.Builder(originalByteBuffer);
    }

    public SimpleTimeSeriesView create(
        ByteBuffer rowIndexUncompressedByteBuffer,
        ByteBuffer dataUncompressedByteBuffer,
        SimpleTimeSeriesObjectStrategy objectStrategy
    )
    {
      SimpleTimeSeriesView simpleTimeSeriesView = new SimpleTimeSeriesView(
          rowReaderBuilder.build(rowIndexUncompressedByteBuffer, dataUncompressedByteBuffer), objectStrategy
      );

      return simpleTimeSeriesView;
    }

    public SimpleTimeSeriesView create(ByteBuffer rowIndexUncompressedBlock, ByteBuffer dataUncompressedBlock)
    {
      return create(
          rowIndexUncompressedBlock,
          dataUncompressedBlock,
          SimpleTimeSeriesComplexMetricSerde.SIMPLE_TIME_SERIES_OBJECT_STRATEGY
      );
    }
  }
}
