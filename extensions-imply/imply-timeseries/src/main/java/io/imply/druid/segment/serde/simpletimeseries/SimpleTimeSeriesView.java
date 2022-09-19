/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.SimpleTimeSeriesBuffer;

import java.nio.ByteBuffer;

public class SimpleTimeSeriesView
{
  public static final ByteBuffer NULL_ROW = ByteBuffer.wrap(new byte[0]);

  private final RowReader rowReader;
  private final SimpleTimeSeriesSerde timeSeriesSerde;

  public SimpleTimeSeriesView(RowReader rowReader, SimpleTimeSeriesSerde timeSeriesSerde)
  {
    this.rowReader = rowReader;
    this.timeSeriesSerde = timeSeriesSerde;
  }

  public SimpleTimeSeriesBuffer getRow(int rowNumber)
  {
    return new SimpleTimeSeriesBuffer(timeSeriesSerde, () -> rowReader.getRow(rowNumber));
  }

  public static class Factory
  {
    private final SimpleTimeSeriesSerde simpleTimeSeriesSerde;
    private final RowReader.Builder rowReaderBuilder;

    public Factory(ByteBuffer originalByteBuffer, SimpleTimeSeriesSerde simpleTimeSeriesSerde)
    {
      this.simpleTimeSeriesSerde = simpleTimeSeriesSerde;
      rowReaderBuilder = new RowReader.Builder(originalByteBuffer);
    }

    public SimpleTimeSeriesView create(ByteBuffer rowIndexUncompressedByteBuffer, ByteBuffer dataUncompressedByteBuffer)
    {
      SimpleTimeSeriesView simpleTimeSeriesView = new SimpleTimeSeriesView(
          rowReaderBuilder.build(rowIndexUncompressedByteBuffer, dataUncompressedByteBuffer), simpleTimeSeriesSerde
      );

      return simpleTimeSeriesView;
    }
  }
}
