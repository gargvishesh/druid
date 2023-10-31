/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.imply.druid.timeseries.SimpleTimeSeriesBuffer;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.serde.cell.CellReader;
import org.apache.druid.segment.serde.cell.NativeClearedByteBufferProvider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleTimeSeriesComplexColumn implements ComplexColumn
{
  public static final byte EXPECTED_VERSION = 0;

  private final Closer closer;
  private final int serializedSize;
  private final CellReader cellReader;
  private final SimpleTimeSeriesSerde encodedTimestampSerde;

  private SimpleTimeSeriesComplexColumn(
      CellReader cellReader,
      SimpleTimeSeriesSerde encodedTimestampSerde,
      Closer closer,
      int serializedSize
  )
  {
    this.cellReader = cellReader;
    this.encodedTimestampSerde = encodedTimestampSerde;
    this.closer = closer;
    this.serializedSize = serializedSize;
  }

  @Override
  public Class<?> getClazz()
  {
    return SimpleTimeSeriesContainer.class;
  }

  @Override
  public String getTypeName()
  {
    return SimpleTimeSeriesContainerComplexMetricSerde.TYPE_NAME;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    SimpleTimeSeriesBuffer buffer = new SimpleTimeSeriesBuffer(encodedTimestampSerde, () -> cellReader.getCell(rowNum));
    return SimpleTimeSeriesContainer.createFromBuffer(buffer);
  }

  @Override
  public int getLength()
  {
    return serializedSize;
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  public static class Factory
  {
    private final int serializedSize;
    private final SimpleTimeSeriesSerde simpleTimeSeriesSerde;
    private final ByteBuffer masterByteBuffer;

    public Factory(ByteBuffer buffer)
    {
      masterByteBuffer = buffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
      serializedSize = masterByteBuffer.remaining();

      SerializedColumnHeader columnHeader = SerializedColumnHeader.fromBuffer(masterByteBuffer);

      Preconditions.checkArgument(
          columnHeader.getVersion() == EXPECTED_VERSION,
          "version %s expected, got %s",
          EXPECTED_VERSION,
          columnHeader.getVersion()
      );
      simpleTimeSeriesSerde = columnHeader.createSimpleTimeSeriesSerde();
    }

    public SimpleTimeSeriesComplexColumn create()
    {
      Closer closer = Closer.create();
      ResourceHolder<ByteBuffer> rowIndexUncompressedBlockHolder = NativeClearedByteBufferProvider.INSTANCE.get();
      ResourceHolder<ByteBuffer> dataUncompressedBlockHolder = NativeClearedByteBufferProvider.INSTANCE.get();
      CellReader cellReader = new CellReader.Builder(masterByteBuffer).build();

      closer.register(cellReader);
      closer.register(rowIndexUncompressedBlockHolder);
      closer.register(dataUncompressedBlockHolder);


      return new SimpleTimeSeriesComplexColumn(cellReader, simpleTimeSeriesSerde, closer, serializedSize);
    }
  }
}
