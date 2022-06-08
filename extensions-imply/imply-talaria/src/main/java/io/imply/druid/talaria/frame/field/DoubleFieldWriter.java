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
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

/**
 * Wraps a {@link BaseDoubleColumnValueSelector} and writes field values.
 *
 * Values are transformed such that they are comparable as bytes; see {@link #transform} and {@link #detransform}.
 * Values are preceded by a null byte that is either 0x00 (null) or 0x01 (not null). This ensures that nulls sort
 * earlier than nonnulls.
 */
public class DoubleFieldWriter implements FieldWriter
{
  public static final int SIZE = Double.BYTES + Byte.BYTES;
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseDoubleColumnValueSelector selector;

  public DoubleFieldWriter(final BaseDoubleColumnValueSelector selector)
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
      memory.putLong(position + Byte.BYTES, transform(0));
    } else {
      memory.putByte(position, NOT_NULL_BYTE);
      memory.putLong(position + Byte.BYTES, transform(selector.getDouble()));
    }

    return SIZE;
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  public static long transform(final double n)
  {
    final long bits = Double.doubleToLongBits(n);
    final long mask = ((bits & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Long.reverseBytes(bits ^ mask);
  }

  public static double detransform(final long bits)
  {
    final long reversedBits = Long.reverseBytes(bits);
    final long mask = (((reversedBits ^ Long.MIN_VALUE) & Long.MIN_VALUE) >> 11) | Long.MIN_VALUE;
    return Double.longBitsToDouble(reversedBits ^ mask);
  }
}
