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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * <pre>
 *  serialized data is of the form:
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
  private final SimpleTimeSeriesObjectStrategy objectStrategy;

  private RowWriter rowWriter;
  private State state = State.START;
  private RowWriter.Builder rowWriterBuilder;

  public SimpleTimeSeriesColumnSerializer(
      RowWriter.Builder rowWriterBuilder,
      SimpleTimeSeriesObjectStrategy objectStrategy
  )
  {
    this.rowWriterBuilder = rowWriterBuilder;
    this.objectStrategy = objectStrategy;
  }

  public SimpleTimeSeriesColumnSerializer(RowWriter.Builder rowWriterBuilder)
  {
    this(rowWriterBuilder, SimpleTimeSeriesComplexMetricSerde.SIMPLE_TIME_SERIES_OBJECT_STRATEGY);
  }


  @Override
  public void open() throws IOException
  {
    Preconditions.checkState(state == State.START || state == State.OPEN, "open called in invalid state %s", state);

    if (state == State.START) {
      rowWriter = rowWriterBuilder.build();
      state = State.OPEN;
    }
  }

  @Override
  public void serialize(ColumnValueSelector<? extends SimpleTimeSeries> selector) throws IOException
  {
    Preconditions.checkState(state == State.OPEN, "serialize called in invalid state %s", state);

    SimpleTimeSeries timeSeries = selector.getObject();
    byte[] rowBytes = objectStrategy.toBytes(timeSeries);

    rowWriter.write(rowBytes);
  }


  @Override
  public long getSerializedSize() throws IOException
  {
    Preconditions.checkState(
        state != State.START,
        "getSerializedSize called in invalid state %s (must have opened at least)",
        state
    );

    if (state == State.OPEN) {
      rowWriter.close();
      state = State.INTERMEDIATE_CLOSED;
    }

    return rowWriter.getSerializedSize();
  }


  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(
        state == State.OPEN || state == State.INTERMEDIATE_CLOSED || state == State.FINAL_CLOSED,
        "writeTo called in invalid state %s",
        state
    );

    if (state != State.INTERMEDIATE_CLOSED) {
      rowWriter.close();
    }

    state = State.FINAL_CLOSED;
    rowWriter.writeTo(channel, smoosher);
  }

  private enum State
  {
    START,
    OPEN,
    INTERMEDIATE_CLOSED,
    FINAL_CLOSED;
  }
}






















