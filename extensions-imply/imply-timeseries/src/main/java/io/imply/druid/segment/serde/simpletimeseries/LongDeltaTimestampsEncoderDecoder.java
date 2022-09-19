/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.utils.ImplyLongArrayList;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;

public class LongDeltaTimestampsEncoderDecoder implements TimestampsEncoderDecoder
{
  private final LongDeltaEncoderDecoder deltaEncoder;

  public LongDeltaTimestampsEncoderDecoder(LongDeltaEncoderDecoder deltaEncoder)
  {
    this.deltaEncoder = deltaEncoder;
  }

  @Override
  public StorableList encode(@Nonnull ImplyLongArrayList timestamps)
  {
    long[] deltas = deltaEncoder.encodeDeltas(timestamps.getLongArray(), timestamps.size());
    List<LongValueRunLengthEncoderDecoder.LongValueRun> encodedList =
        LongValueRunLengthEncoderDecoder.encode(deltas, deltas.length);
    int runLengthEnocdedSize = getSerializedSizeOfRunLengthEncoded(encodedList);
    int rawValuesSize = getSerializedSizeOfRawValues(deltas);

    return runLengthEnocdedSize > rawValuesSize ? new RawDeltaList(deltas) : new RunLengthEncoded(encodedList);
  }

  @Override
  public ImplyLongArrayList decode(@Nonnull ByteBuffer byteBuffer, boolean isRle)
  {
    if (byteBuffer.remaining() == 0) {
      return new ImplyLongArrayList(0);
    }

    if (isRle) {
      long[] timestampValues = LongValueRunLengthEncoderDecoder.decode(byteBuffer);
      deltaEncoder.decodeDeltas(timestampValues);

      return new ImplyLongArrayList(timestampValues);
    } else {
      return decodeRawList(byteBuffer);
    }
  }

  @Nonnull
  private ImplyLongArrayList decodeRawList(ByteBuffer byteBuffer)
  {
    int count = byteBuffer.getInt();
    long[] timestampValues = new long[count];

    byteBuffer.asLongBuffer().get(timestampValues);
    byteBuffer.position(byteBuffer.position() + Long.BYTES * count);
    deltaEncoder.decodeDeltas(timestampValues);

    return new ImplyLongArrayList(timestampValues);
  }


  private static int getSerializedSizeOfRunLengthEncoded(List<LongValueRunLengthEncoderDecoder.LongValueRun> encodedList)
  {
    return Integer.BYTES + encodedList.size() * LongValueRunLengthEncoderDecoder.LongValueRun.SERIALIZED_SIZE;
  }

  private static int getSerializedSizeOfRawValues(long[] deltas)
  {
    return Integer.BYTES + Long.BYTES * deltas.length;
  }

  private static class RunLengthEncoded implements StorableList
  {
    private final List<LongValueRunLengthEncoderDecoder.LongValueRun> encodedList;

    public RunLengthEncoded(List<LongValueRunLengthEncoderDecoder.LongValueRun> encodedList)
    {
      this.encodedList = encodedList;
    }

    @Override
    public boolean isRle()
    {
      return true;
    }

    @Override
    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(encodedList.size());
      encodedList.forEach(e -> e.store(byteBuffer));
    }

    @Override
    public int getSerializedSize()
    {
      return getSerializedSizeOfRunLengthEncoded(encodedList);
    }
  }

  private static class RawDeltaList implements StorableList
  {
    private final long[] deltas;

    public RawDeltaList(long[] deltas)
    {
      this.deltas = deltas;
    }

    @Override
    public boolean isRle()
    {
      return false;
    }

    @Override
    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(deltas.length);
      byteBuffer.asLongBuffer().put(deltas);
      byteBuffer.position(byteBuffer.position() + Long.BYTES * deltas.length);
    }

    @Override
    public int getSerializedSize()
    {
      return getSerializedSizeOfRawValues(deltas);
    }
  }
}
