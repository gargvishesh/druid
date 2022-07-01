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

public class IntegerValueRunLengthEncoderDecoder
{
  private IntegerValueRunLengthEncoderDecoder()
  {
  }

  /**
   * encodes elements 0...(length - 1)
   */
  public static List<IntegerValueRun> encode(int[] values, int length)
  {
    if (length == 0) {
      return Collections.emptyList();
    }

    List<IntegerValueRun> valueRunList = new ArrayList<>(length);
    IntegerValueRun currentValueRun = null;

    for (int i = 0; i < length; i++) {
      int value = values[i];

      if (currentValueRun == null) {
        currentValueRun = new IntegerValueRun(1, value);
      } else if (currentValueRun.value == value) {
        if (currentValueRun.length + 1L > Integer.MAX_VALUE) {
          valueRunList.add(currentValueRun);
          currentValueRun = new IntegerValueRun(1, value);
        } else {
          currentValueRun.length++;
        }
      } else {
        valueRunList.add(currentValueRun);
        currentValueRun = new IntegerValueRun(1, value);
      }
    }

    if (currentValueRun.length > 0) {
      valueRunList.add(currentValueRun);
    }

    return valueRunList;
  }

  public static long[] decode(ByteBuffer byteBuffer)
  {
    if (byteBuffer.remaining() == 0) {
      return new long[0];
    }

    int count = byteBuffer.getInt();

    if (count == 0) {
      return new long[0];
    }

    List<IntegerValueRun> valueRunList = new ArrayList<>(count);
    int decodedCount = 0;

    for (int i = 0; i < count; i++) {
      IntegerValueRun valueRun = IntegerValueRun.create(byteBuffer);

      decodedCount += valueRun.length;
      valueRunList.add(valueRun);
    }

    long[] decoded = new long[decodedCount];
    int position = 0;

    for (IntegerValueRun valueRun : valueRunList) {
      for (int j = 0; j < valueRun.length; j++) {
        decoded[position++] = valueRun.value;
      }
    }

    return decoded;
  }

  public static class IntegerValueRun
  {
    public static final int SERIALIZED_SIZE = Integer.BYTES + Integer.BYTES;

    private final int value;
    private int length;

    public IntegerValueRun(int length, int value)
    {
      this.length = length;
      this.value = value;
    }

    public static IntegerValueRun create(ByteBuffer byteBuffer)
    {
      int length = byteBuffer.getInt();
      int value = byteBuffer.getInt();

      return new IntegerValueRun(length, value);
    }

    public int getLength()
    {
      return length;
    }

    public int getValue()
    {
      return value;
    }

    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(length);
      byteBuffer.putInt(value);
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
      IntegerValueRun that = (IntegerValueRun) o;
      return length == that.length && value == that.value;
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
