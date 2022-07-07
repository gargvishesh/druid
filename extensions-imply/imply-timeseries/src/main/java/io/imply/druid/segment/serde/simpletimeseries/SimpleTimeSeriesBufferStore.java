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
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;
import java.util.OptionalLong;

public class SimpleTimeSeriesBufferStore
{
  private static final SimpleTimeSeriesObjectStrategy TIME_SERIES_OBJECT_STRATEGY =
      new SimpleTimeSeriesObjectStrategy();

  private final WriteOutBytes writeOutBytes;
  private final IntSerializer intSerializer = new IntSerializer();

  private long minTimeStamp = Long.MAX_VALUE;
  private long maxTimeStamp = Long.MIN_VALUE;

  public SimpleTimeSeriesBufferStore(WriteOutBytes writeOutBytes)
  {
    this.writeOutBytes = writeOutBytes;
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

    byte[] bytes = TIME_SERIES_OBJECT_STRATEGY.toBytes(simpleTimeSeries);

    writeOutBytes.write(intSerializer.serialize(bytes.length));
    writeOutBytes.write(bytes);
  }

  public TransferredBuffer transferToRowWriter(
      NativeClearedByteBufferProvider byteBufferProvider,
      SegmentWriteOutMedium segmentWriteOutMedium
  ) throws IOException
  {
    SerializedColumnHeader columnHeader = createColumnHeader();
    SimpleTimeSeriesSerde timeSeriesSerde = columnHeader.createSimpleTimeSeriesSerde();
    RowWriter rowWriter = new RowWriter.Builder(byteBufferProvider, segmentWriteOutMedium).build();
    IOIterator<SimpleTimeSeries> bufferIterator = iterator();

    while (bufferIterator.hasNext()) {
      SimpleTimeSeries timeSeries = bufferIterator.next();
      // TODO: we *could* re-use a ByteBuffer -or- byte[] object of a large size and only allocate this object
      // if the serialized size exceeds the cached buffer/byte[]
      byte[] serialized = timeSeriesSerde.serialize(timeSeries);

      rowWriter.write(serialized);
    }

    rowWriter.close();


    return new TransferredBuffer(rowWriter, columnHeader);
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
    return new SimpleTimeSeriesBufferIterator(writeOutBytes.asInputStream());
  }

  public static class TransferredBuffer implements Serializer
  {
    private final RowWriter rowWriter;
    private final SerializedColumnHeader columnHeader;

    public TransferredBuffer(RowWriter rowWriter, SerializedColumnHeader columnHeader)
    {
      this.rowWriter = rowWriter;
      this.columnHeader = columnHeader;
    }

    public RowWriter getRowWriter()
    {
      return rowWriter;
    }

    public SerializedColumnHeader getColumnHeader()
    {
      return columnHeader;
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      columnHeader.transferTo(channel);
      rowWriter.writeTo(channel, smoosher);
    }

    @Override
    public long getSerializedSize()
    {
      return columnHeader.getSerializedSize() + rowWriter.getSerializedSize();
    }
  }

  private static class SimpleTimeSeriesBufferIterator implements IOIterator<SimpleTimeSeries>
  {
    private static final int NEEDS_READ = -2;
    private static final int EOF = -1;

    private final byte[] intBytes;
    private final BufferedInputStream inputStream;

    private int nextSize;

    public SimpleTimeSeriesBufferIterator(InputStream inputStream)
    {
      this.inputStream = new BufferedInputStream(inputStream);
      intBytes = new byte[Integer.BYTES];
      nextSize = NEEDS_READ;
    }

    @Override
    public boolean hasNext() throws IOException
    {
      return getNextSize() > EOF;
    }

    @Override
    public SimpleTimeSeries next() throws IOException
    {
      int currentNextSize = getNextSize();

      if (currentNextSize == -1) {
        throw new NoSuchElementException("end of buffer reached");
      }

      byte[] nextBytes = new byte[currentNextSize];
      int bytesRead = 0;

      while (bytesRead < currentNextSize) {
        int result = inputStream.read(nextBytes, bytesRead, currentNextSize - bytesRead);

        if (result == -1) {
          throw new NoSuchElementException("unexpected end of buffer reached");
        }

        bytesRead += result;
      }


      SimpleTimeSeries simpleTimeSeries = TIME_SERIES_OBJECT_STRATEGY.fromByteBuffer(
          ByteBuffer.wrap(nextBytes).order(ByteOrder.nativeOrder()), nextBytes.length
      );

      nextSize = NEEDS_READ;


      return simpleTimeSeries;
    }

    private int getNextSize() throws IOException
    {
      if (nextSize == NEEDS_READ) {
        int bytesRead = 0;
        while (bytesRead < Integer.BYTES) {
          int result = inputStream.read(intBytes);

          if (result == -1) {
            nextSize = EOF;
            return EOF;
          } else {
            bytesRead += result;
          }
        }

        nextSize = ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).getInt();
      }

      return nextSize;
    }
  }
}
