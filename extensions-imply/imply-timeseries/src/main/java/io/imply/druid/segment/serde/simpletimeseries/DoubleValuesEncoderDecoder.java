/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;

public class DoubleValuesEncoderDecoder
{
  private DoubleValuesEncoderDecoder()
  {
  }

  public static StorableList encode(ImplyDoubleArrayList values)
  {
    List<DoubleValueRunLengthEncoderDecoder.DoubleValueRun> encodedList =
        DoubleValueRunLengthEncoderDecoder.encode(values.getDoubleArray(), values.size());
    int runLengthEncodedSize = getSerializedSizeOfRunLengthEncoded(encodedList);
    int rawSize = getSerializedSizeOfRawValueList(values);

    return runLengthEncodedSize > rawSize ? new RawValueList(values) : new RunLengthEncoded(encodedList);
  }

  public static ImplyDoubleArrayList decode(ByteBuffer byteBuffer, boolean isRle)
  {
    if (byteBuffer.remaining() == 0) {
      return new ImplyDoubleArrayList(0);
    }
    if (isRle) {
      double[] decoded = DoubleValueRunLengthEncoderDecoder.decode(byteBuffer);

      return new ImplyDoubleArrayList(decoded);
    } else {
      return decodeRawList(byteBuffer);
    }
  }

  @Nonnull
  private static ImplyDoubleArrayList decodeRawList(ByteBuffer byteBuffer)
  {
    int count = byteBuffer.getInt();
    double[] doubleValues = new double[count];

    byteBuffer.asDoubleBuffer().get(doubleValues);
    byteBuffer.position(byteBuffer.position() + Double.BYTES * count);

    ImplyDoubleArrayList datapoints = new ImplyDoubleArrayList(doubleValues);

    return datapoints;
  }

  private static int getSerializedSizeOfRawValueList(ImplyDoubleArrayList values)
  {
    // count + size_of(double[])
    return Integer.BYTES + values.size() * Double.BYTES;
  }

  private static int getSerializedSizeOfRunLengthEncoded(List<DoubleValueRunLengthEncoderDecoder.DoubleValueRun> encodedList)
  {
    // count + size_of(List<DoubleValueRun>)
    return Integer.BYTES + DoubleValueRunLengthEncoderDecoder.DoubleValueRun.SERIALIZED_SIZE * encodedList.size();
  }

  private static class RunLengthEncoded implements StorableList
  {
    private final List<DoubleValueRunLengthEncoderDecoder.DoubleValueRun> encodedList;

    public RunLengthEncoded(List<DoubleValueRunLengthEncoderDecoder.DoubleValueRun> encodedList)
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

  private static class RawValueList implements StorableList
  {
    private final ImplyDoubleArrayList values;

    public RawValueList(ImplyDoubleArrayList values)
    {
      this.values = values;
    }

    @Override
    public boolean isRle()
    {
      return false;
    }

    @Override
    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(values.size());
      byteBuffer.asDoubleBuffer().put(values.getDoubleArray(), 0, values.size());
      byteBuffer.position(byteBuffer.position() + Double.BYTES * values.size());
    }

    @Override
    public int getSerializedSize()
    {
      return getSerializedSizeOfRawValueList(values);
    }
  }
}
