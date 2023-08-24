/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LongValueRunLengthEncoderDecoder
{
  private LongValueRunLengthEncoderDecoder()
  {
  }

  /**
   * encodes elements 0...(length - 1)
   */
  public static List<LongValueRun> encode(long[] values, int length)
  {
    if (length == 0) {
      return Collections.emptyList();
    }

    List<LongValueRun> valueRunList = new ArrayList<>(length);
    LongValueRun currentValueRun = null;

    for (int i = 0; i < length; i++) {
      long value = values[i];

      if (currentValueRun == null) {
        currentValueRun = new LongValueRun(1, value);
      } else if (currentValueRun.value == value) {
        if (currentValueRun.length + 1L > Integer.MAX_VALUE) {
          valueRunList.add(currentValueRun);
          currentValueRun = new LongValueRun(1, value);
        } else {
          currentValueRun.length++;
        }
      } else {
        valueRunList.add(currentValueRun);
        currentValueRun = new LongValueRun(1, value);
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

    List<LongValueRun> valueRunList = new ArrayList<>(count);
    int decodedCount = 0;

    for (int i = 0; i < count; i++) {
      LongValueRun valueRun = LongValueRun.create(byteBuffer);

      decodedCount += valueRun.length;
      valueRunList.add(valueRun);
    }

    long[] decoded = new long[decodedCount];
    int position = 0;

    for (LongValueRun valueRun : valueRunList) {
      for (int j = 0; j < valueRun.length; j++) {
        decoded[position++] = valueRun.value;
      }
    }

    return decoded;
  }

  public static class LongValueRun
  {
    public static final int SERIALIZED_SIZE = Integer.BYTES + Long.BYTES;

    private final long value;
    private int length;

    public LongValueRun(int length, long value)
    {
      this.length = length;
      this.value = value;
    }

    public static LongValueRun create(ByteBuffer byteBuffer)
    {
      int length = byteBuffer.getInt();
      long value = byteBuffer.getLong();

      return new LongValueRun(length, value);
    }

    public int getLength()
    {
      return length;
    }

    public long getValue()
    {
      return value;
    }

    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.putInt(length);
      byteBuffer.putLong(value);
    }

    @SuppressWarnings("NonFinalFieldReferenceInEquals")
    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LongValueRun that = (LongValueRun) o;
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
      return MoreObjects.toStringHelper(this)
                        .add("length", length)
                        .add("value", value)
                        .toString();
    }
  }
}
