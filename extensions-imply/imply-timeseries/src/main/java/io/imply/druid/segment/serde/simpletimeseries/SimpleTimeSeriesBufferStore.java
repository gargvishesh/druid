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
import io.imply.druid.timeseries.SimpleTimeSeries;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.aggregation.SerializedStorage;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.serde.cell.CellWriter;
import org.apache.druid.segment.serde.cell.IOIterator;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.OptionalLong;

public class SimpleTimeSeriesBufferStore
{
  private final SerializedStorage<SimpleTimeSeries> serializedStorage;

  private long minTimeStamp = Long.MAX_VALUE;
  private long maxTimeStamp = Long.MIN_VALUE;

  public SimpleTimeSeriesBufferStore(SerializedStorage<SimpleTimeSeries> serializedStorage)
  {
    this.serializedStorage = serializedStorage;
  }

  public void store(@Nullable SimpleTimeSeries simpleTimeSeries) throws IOException
  {
    if (simpleTimeSeries != null && !simpleTimeSeries.getTimestamps().isEmpty()) {
      OptionalLong minForThisSeries = simpleTimeSeries.getTimestamps().longStream().min();
      OptionalLong maxForThisSeries = simpleTimeSeries.getTimestamps().longStream().max();

      minTimeStamp = Math.min(
          minTimeStamp,
          minForThisSeries.orElseThrow(() -> new RuntimeException("should never see this (min)"))
      );
      maxTimeStamp = Math.max(
          maxTimeStamp,
          maxForThisSeries.orElseThrow(() -> new RuntimeException("should never see this (max)"))
      );
    }

    serializedStorage.store(simpleTimeSeries);
  }

  public TransferredBuffer transferToRowWriter(SegmentWriteOutMedium segmentWriteOutMedium) throws IOException
  {
    SerializedColumnHeader columnHeader = createColumnHeader();
    SimpleTimeSeriesSerde timeSeriesSerde =
        SimpleTimeSeriesSerde.create(columnHeader.getMinTimestamp(), columnHeader.isUseIntegerDeltas());
    try (CellWriter cellWriter = new CellWriter.Builder(segmentWriteOutMedium).build();
         IOIterator<SimpleTimeSeries> bufferIterator = serializedStorage.iterator()) {

      while (bufferIterator.hasNext()) {
        SimpleTimeSeries timeSeries = bufferIterator.next();
        byte[] serialized = timeSeriesSerde.serialize(timeSeries);

        cellWriter.write(serialized);
      }

      return new TransferredBuffer(cellWriter, columnHeader);
    }
  }

  @Nonnull
  public SerializedColumnHeader createColumnHeader()
  {
    long maxDelta = maxTimeStamp - minTimeStamp;
    SerializedColumnHeader columnHeader;

    Preconditions.checkState(maxDelta >= 0, "attempt to transfer empty buffer");

    if (maxDelta <= Integer.MAX_VALUE) {
      columnHeader = new SerializedColumnHeader(SimpleTimeSeriesComplexColumn.EXPECTED_VERSION, true, minTimeStamp);
    } else {
      columnHeader = new SerializedColumnHeader(SimpleTimeSeriesComplexColumn.EXPECTED_VERSION, false, minTimeStamp);
    }
    return columnHeader;
  }

  public IOIterator<SimpleTimeSeries> iterator() throws IOException
  {
    return serializedStorage.iterator();
  }

  public static class TransferredBuffer implements Serializer
  {
    private final CellWriter cellWriter;
    private final SerializedColumnHeader columnHeader;

    public TransferredBuffer(CellWriter cellWriter, SerializedColumnHeader columnHeader)
    {
      this.cellWriter = cellWriter;
      this.columnHeader = columnHeader;
    }

    public CellWriter getCellWriter()
    {
      return cellWriter;
    }

    public SerializedColumnHeader getColumnHeader()
    {
      return columnHeader;
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      columnHeader.transferTo(channel);
      cellWriter.writeTo(channel, smoosher);
    }

    @Override
    public long getSerializedSize()
    {
      return columnHeader.getSerializedSize() + cellWriter.getSerializedSize();
    }
  }
}
