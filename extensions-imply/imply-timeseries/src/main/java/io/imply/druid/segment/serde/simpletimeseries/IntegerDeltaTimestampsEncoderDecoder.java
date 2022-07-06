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

public class IntegerDeltaTimestampsEncoderDecoder implements TimestampsEncoderDecoder
{
  private final IntegerDeltaEncoderDecoder deltaEncoderDecoder;

  public IntegerDeltaTimestampsEncoderDecoder(IntegerDeltaEncoderDecoder deltaEncoderDecoder)
  {
    this.deltaEncoderDecoder = deltaEncoderDecoder;
  }

  @Override
  public StorableList encode(@Nonnull ImplyLongArrayList timestamps)
  {
    int[] deltas = deltaEncoderDecoder.encodeDeltas(timestamps.getLongArray(), timestamps.size());
    List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encodedList =
        IntegerValueRunLengthEncoderDecoder.encode(deltas, deltas.length);
    int runLengthEncodedSize = getSerializedSizeOfRunLengthEncoded(encodedList);
    int rawDeltasSize = getSerializedSizeOfRawDeltaList(deltas);

    return runLengthEncodedSize > rawDeltasSize ? new RawDeltaList(deltas) : new RunLengthEncoded(encodedList);
  }

  @Override
  public ImplyLongArrayList decode(@Nonnull ByteBuffer byteBuffer, boolean isRle)
  {
    if (byteBuffer.remaining() == 0) {
      return new ImplyLongArrayList(0);
    }

    if (isRle) {
      long[] timestampValues = IntegerValueRunLengthEncoderDecoder.decode(byteBuffer);
      deltaEncoderDecoder.decodeDeltas(timestampValues);

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

    for (int i = 0; i < timestampValues.length; i++) {
      timestampValues[i] = byteBuffer.getInt();
    }

    deltaEncoderDecoder.decodeDeltas(timestampValues);

    return new ImplyLongArrayList(timestampValues);
  }

  private static int getSerializedSizeOfRunLengthEncoded(List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encodedList)
  {
    //count + sizeof(List<RLE>)
    return Integer.BYTES + IntegerValueRunLengthEncoderDecoder.IntegerValueRun.SERIALIZED_SIZE * encodedList.size();
  }

  private static int getSerializedSizeOfRawDeltaList(int[] deltas)
  {
    // count + sizeof(int[])
    return Integer.BYTES + Integer.BYTES * deltas.length;
  }

  private static class RunLengthEncoded implements StorableList
  {
    private final List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encodedList;

    public RunLengthEncoded(List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encodedList)
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
    private final int[] deltas;

    public RawDeltaList(int[] deltas)
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
      byteBuffer.asIntBuffer().put(deltas);
      byteBuffer.position(byteBuffer.position() + Integer.BYTES * deltas.length);
    }

    @Override
    public int getSerializedSize()
    {
      return getSerializedSizeOfRawDeltaList(deltas);
    }
  }
}
