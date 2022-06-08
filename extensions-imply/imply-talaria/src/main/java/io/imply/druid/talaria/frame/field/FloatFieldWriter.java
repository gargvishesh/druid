/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

/**
 * Wraps a {@link BaseFloatColumnValueSelector} and writes field values.
 *
 * Values are transformed such that they are comparable as bytes; see {@link #transform} and {@link #detransform}.
 * Values are preceded by a null byte that is either 0x00 (null) or 0x01 (not null). This ensures that nulls sort
 * earlier than nonnulls.
 */
public class FloatFieldWriter implements FieldWriter
{
  public static final int SIZE = Float.BYTES + Byte.BYTES;
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseFloatColumnValueSelector selector;

  public FloatFieldWriter(final BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    if (maxSize < SIZE) {
      return -1;
    }

    if (selector.isNull()) {
      memory.putByte(position, NULL_BYTE);
      memory.putInt(position + Byte.BYTES, transform(0));
    } else {
      memory.putByte(position, NOT_NULL_BYTE);
      memory.putInt(position + Byte.BYTES, transform(selector.getFloat()));
    }

    return SIZE;
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  public static int transform(final float n)
  {
    final int bits = Float.floatToIntBits(n);
    final int mask = ((bits & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Integer.reverseBytes(bits ^ mask);
  }

  public static float detransform(final int bits)
  {
    final int reversedBits = Integer.reverseBytes(bits);
    final int mask = (((reversedBits ^ Integer.MIN_VALUE) & Integer.MIN_VALUE) >> 8) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(reversedBits ^ mask);
  }
}
