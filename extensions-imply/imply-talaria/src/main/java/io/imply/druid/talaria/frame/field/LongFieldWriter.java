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
import org.apache.druid.segment.BaseLongColumnValueSelector;

/**
 * Wraps a {@link BaseLongColumnValueSelector} and writes individual values into rframe rows.
 *
 * Longs are written in big-endian order, with the sign bit flipped, so they can be compared as bytes. Longs are
 * preceded by a null byte that is either 0x00 (null) or 0x01 (not null). This ensures that nulls sort earlier than
 * nonnulls.
 */
public class LongFieldWriter implements FieldWriter
{
  public static final int SIZE = Long.BYTES + Byte.BYTES;
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseLongColumnValueSelector selector;

  public LongFieldWriter(final BaseLongColumnValueSelector selector)
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
      memory.putLong(position + Byte.BYTES, 0 /* no need to call reverseBytes on zero */);
    } else {
      memory.putByte(position, NOT_NULL_BYTE);

      // Must flip the first (sign) bit so comparison-as-bytes works.
      memory.putLong(position + Byte.BYTES, Long.reverseBytes(selector.getLong() ^ Long.MIN_VALUE));
    }

    return SIZE;
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
