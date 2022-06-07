/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Throwables;
import io.imply.druid.timeseries.SimpleTimeSeries;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ComplexColumn;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesComplexColumn implements ComplexColumn
{
  private final SimpleTimeSeriesView simpleTimeSeriesView;
  private final Closer closer;
  private int serializedSize;

  private SimpleTimeSeriesComplexColumn(SimpleTimeSeriesView simpleTimeSeriesView, Closer closer, int serializedSize)
  {
    this.simpleTimeSeriesView = simpleTimeSeriesView;
    this.closer = closer;
    this.serializedSize = serializedSize;
  }

  public static SimpleTimeSeriesComplexColumn create(
      SimpleTimeSeriesView.Factory simpleTimeSeriesViewFactory,
      NativeClearedByteBufferProvider byteBufferProvider,
      int serializedSize
  )
  {
    Closer closer = Closer.create();
    ResourceHolder<ByteBuffer> rowIndexUncompressedBlockHolder = byteBufferProvider.get();
    ResourceHolder<ByteBuffer> dataUncompressedBlockHolder = byteBufferProvider.get();

    closer.register(rowIndexUncompressedBlockHolder);
    closer.register(dataUncompressedBlockHolder);

    SimpleTimeSeriesView simpleTimeSeriesView = simpleTimeSeriesViewFactory.create(
        rowIndexUncompressedBlockHolder.get(), dataUncompressedBlockHolder.get()
    );

    return new SimpleTimeSeriesComplexColumn(simpleTimeSeriesView, closer, serializedSize);
  }

  @Override
  public Class<?> getClazz()
  {
    return SimpleTimeSeries.class;
  }

  @Override
  public String getTypeName()
  {
    return SimpleTimeSeriesComplexMetricSerde.TYPE_NAME;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    return simpleTimeSeriesView.getRow(rowNum);
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
}
