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
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.serde.ComplexMetricSerde;

/**
 * Wraps a {@link BaseObjectColumnValueSelector} and uses {@link ComplexMetricSerde#toBytes} to write complex objects.
 */
public class ComplexFieldWriter implements FieldWriter
{
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  static final int HEADER_SIZE = Byte.BYTES /* null byte */ + Integer.BYTES /* length */;

  private final ComplexMetricSerde serde;
  private final BaseObjectColumnValueSelector<?> selector;

  ComplexFieldWriter(
      final ComplexMetricSerde serde,
      final BaseObjectColumnValueSelector<?> selector
  )
  {
    this.serde = serde;
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    final Object complexObject = selector.getObject();

    if (maxSize < HEADER_SIZE) {
      return -1;
    }

    if (complexObject == null) {
      memory.putByte(position, NULL_BYTE);
      memory.putInt(position + Byte.BYTES, 0);
      return HEADER_SIZE;
    } else {
      final byte[] bytes = serde.toBytes(complexObject);
      final int fieldLength = HEADER_SIZE + bytes.length;

      if (maxSize < fieldLength) {
        return -1;
      } else {
        memory.putByte(position, NOT_NULL_BYTE);
        memory.putInt(position + Byte.BYTES, bytes.length);
        memory.putByteArray(position + HEADER_SIZE, bytes, 0, bytes.length);
        return fieldLength;
      }
    }
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
