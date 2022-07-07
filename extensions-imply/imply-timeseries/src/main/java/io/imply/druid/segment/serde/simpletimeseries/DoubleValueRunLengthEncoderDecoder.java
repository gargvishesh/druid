/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Objects;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DoubleValueRunLengthEncoderDecoder
{
  private DoubleValueRunLengthEncoderDecoder()
  {
  }

  /**
   * encodes elements 0...(length - 1)
   */
  public static List<DoubleValueRun> encode(double[] values, int length)
  {
    if (length == 0) {
      return Collections.emptyList();
    }

    List<DoubleValueRun> valueRunList = new ArrayList<>(length);
    DoubleValueRun currentValueRun = null;

    for (int i = 0; i < length; i++) {
      double value = values[i];

      if (currentValueRun == null) {
        currentValueRun = new DoubleValueRun(1, value);
      } else if (currentValueRun.value == value) {
        if (currentValueRun.length + 1L > Integer.MAX_VALUE) {
          valueRunList.add(currentValueRun);
          currentValueRun = new DoubleValueRun(1, value);
        }

        currentValueRun.length++;
      } else {
        valueRunList.add(currentValueRun);
        currentValueRun = new DoubleValueRun(1, value);
      }
    }

    if (currentValueRun.length > 0) {
      valueRunList.add(currentValueRun);
    }

    return valueRunList;
  }

  /**
   * This function <i>changes</i> the position of the byteBuffer. Callers must ensure slice()/asReadOnlyBuffer()
   * have been called
   */
  public static double[] decode(ByteBuffer byteBuffer)
  {
    if (byteBuffer.remaining() == 0) {
      return new double[0];
    }

    int count = byteBuffer.getInt();

    if (count == 0) {
      return new double[0];
    }

    List<DoubleValueRun> valueRunList = new ArrayList<>(count);
    int decodedCount = 0;

    for (int i = 0; i < count; i++) {
      DoubleValueRun valueRun = DoubleValueRun.create(byteBuffer);

      decodedCount += valueRun.length;
      valueRunList.add(valueRun);
    }

    double[] decoded = new double[decodedCount];
    int position = 0;

    for (DoubleValueRun valueRun : valueRunList) {
      for (int j = 0; j < valueRun.length; j++) {
        decoded[position++] = valueRun.value;
      }
    }

    return decoded;
  }

  public static class DoubleValueRun
  {
    public static final int SERIALIZED_SIZE = Integer.BYTES + Double.BYTES;

    private int length;
    private final double value;

    public DoubleValueRun(int length, double value)
    {
      this.length = length;
      this.value = value;
    }

    public static DoubleValueRun create(ByteBuffer byteBuffer)
    {
      int length = byteBuffer.getInt();
      double value = byteBuffer.getDouble();

      return new DoubleValueRun(length, value);
    }

    public int getLength()
    {
      return length;
    }

    public double getValue()
    {
      return value;
    }

    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(length);
      byteBuffer.putDouble(value);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DoubleValueRun that = (DoubleValueRun) o;
      return length == that.length && Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(length, value);
    }

    @Override
    public String toString()
    {
      return Objects.toStringHelper(this)
                        .add("length", length)
                        .add("value", value)
                        .toString();
    }
  }

}
