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
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * <pre>
 * Data is buffered in order to scan the timestamps for optimal delta-encoding. Once serialized() calls are complete,
 * the data will be block-compressed in the following format.
 *
 * serialized data is of the form:
 *
 *    <row index>
 *    <payload storage>
 *
 * each of these items is stored in compressed streams of blocks with a block index.
 *
 * A BlockCompressedPayloadScribe stores byte[] payloads. These may be accessed by creating a
 * BlockCompressedPayloadReader over the produced ByteBuffer. Reads may be done by giving a location in the
 * uncompressed stream and a size
 *
 * * blockIndexSize:int
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * |      block index
 * |      compressed block # -> block start in compressed stream position (relative to data start)
 * |
 * |      0: [block position: int]
 * |      1: [block position: int]
 * |      ...
 * |      i: [block position: int]
 * |      ...
 * |      n: [block position: int]
 * |      n+1: [total compressed size ] // stored to simplify invariant of
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * dataSize:int
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * | <compressed payload block 1>
 * | <compressed payload block 2>
 * | ...
 * | <compressed paylod block n>
 * |
 *
 * the RowIndexWriter stores an array of longs using the BlockCompressedPayloadWriter
 *
 * logically this an array of longs
 *
 * |    0: start_0 : long
 * |    1: start_1 : long
 * |    ...
 * |    n: start_n : long
 * |    n+1: start_n + length_n : long  //ie, next position that would have been written to
 *                                      //used again for invariant of length_i = row_i+1 - row_i
 *
 *      but this will be stored as block compressed. Reads are done by addressing it as a long array of bytes
 *
 * |    <block index size>
 * |    <block index>
 * |
 * |    <data stream size>
 * |    <block compressed payload stream>
 *
 * resulting in
 *
 * |    <row index size>
 * | ----row index------------------------
 * |    <block index size>
 * |    <block index>
 * |    <data stream size>
 * |    <block compressed payload stream>
 * | -------------------------------------
 * |    <data stream size>
 * | ----data stream------------------------
 * |    <block index size>
 * |    <block index>
 * |    <data stream size>
 * |    <block compressed payload stream>
 * | -------------------------------------
 * </pre>
 */

public class SimpleTimeSeriesColumnSerializer implements GenericColumnSerializer<SimpleTimeSeries>
{
  private final NativeClearedByteBufferProvider byteBufferProvider;
  private final SegmentWriteOutMedium segmentWriteOutMedium;

  private State state = State.START;
  private SimpleTimeSeriesBufferStore buffer;
  private SimpleTimeSeriesBufferStore.TransferredBuffer transferredBuffer;

  public SimpleTimeSeriesColumnSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      NativeClearedByteBufferProvider byteBufferProvider
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.byteBufferProvider = byteBufferProvider;
  }


  public SimpleTimeSeriesColumnSerializer(SegmentWriteOutMedium writeOutMedium)
  {
    this(writeOutMedium, NativeClearedByteBufferProvider.DEFAULT);
  }


  @Override
  public void open() throws IOException
  {
    Preconditions.checkState(state == State.START || state == State.OPEN, "open called in invalid state %s", state);

    if (state == State.START) {
      buffer = new SimpleTimeSeriesBufferStore(segmentWriteOutMedium.makeWriteOutBytes());
      state = State.OPEN;
    }
  }

  @Override
  public void serialize(ColumnValueSelector<? extends SimpleTimeSeries> selector) throws IOException
  {
    Preconditions.checkState(state == State.OPEN, "serialize called in invalid state %s", state);

    SimpleTimeSeries timeSeries = selector.getObject();

    buffer.store(timeSeries);
  }


  @Override
  public long getSerializedSize() throws IOException
  {
    Preconditions.checkState(
        state != State.START,
        "getSerializedSize called in invalid state %s (must have opened at least)",
        state
    );

    transferToRowWriterIfNecessary();

    return transferredBuffer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(state != State.START, "writeTo called in invalid state %s", state);
    transferToRowWriterIfNecessary();
    state = State.FINAL_CLOSED;
    transferredBuffer.writeTo(channel, smoosher);
  }

  private void transferToRowWriterIfNecessary() throws IOException
  {
    if (state == State.OPEN) {
      transferredBuffer = buffer.transferToRowWriter(byteBufferProvider, segmentWriteOutMedium);
      state = State.INTERMEDIATE_CLOSED;
    }
  }

  private enum State
  {
    START,
    OPEN,
    INTERMEDIATE_CLOSED,
    FINAL_CLOSED,
  }
}
