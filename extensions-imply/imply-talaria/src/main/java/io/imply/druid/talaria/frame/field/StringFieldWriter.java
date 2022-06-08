/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Wraps a {@link DimensionSelector} and writes to rframe rows.
 *
 * Strings are written in UTF8 and terminated by {@link #VALUE_TERMINATOR}. Note that this byte appears in valid
 * UTF8 encodings if and only if the string contains a NUL (char 0). Therefore, this field writer cannot write
 * out strings containing NUL characters.
 *
 * Rows are terminated by {@link #ROW_TERMINATOR}.
 *
 * Nulls are stored as {@link #NULL_BYTE}. All other strings are prepended by {@link #NOT_NULL_BYTE} byte to
 * differentiate them from nulls.
 *
 * This encoding allows the encoded data to be compared as bytes in a way that matches the behavior of
 * {@link org.apache.druid.segment.StringDimensionHandler#DIMENSION_SELECTOR_COMPARATOR}, except null and
 * empty list are not considered equal.
 */
public class StringFieldWriter implements FieldWriter
{
  public static final byte VALUE_TERMINATOR = (byte) 0x00;
  public static final byte ROW_TERMINATOR = (byte) 0x01;
  public static final byte NULL_BYTE = 0x02;
  public static final byte NOT_NULL_BYTE = 0x03;

  private static final int ROW_OVERHEAD_BYTES = 3; // Null byte + value terminator + row terminator

  private final DimensionSelector selector;

  public StringFieldWriter(final DimensionSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    final List<ByteBuffer> byteBuffers = FrameWriterUtils.getUtf8ByteBuffersFromStringSelector(selector, true);
    return writeUtf8ByteBuffers(memory, position, maxSize, byteBuffers);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  /**
   * Writes a collection of UTF-8 buffers in string-field format. Helper for {@link #writeTo}.
   * All buffers must be nonnull. Null strings must be represented as {@link FrameWriterUtils#NULL_STRING_MARKER_ARRAY}.
   *
   * @return number of bytes written, or -1 if "maxSize" was not enough memory
   */
  static long writeUtf8ByteBuffers(
      final WritableMemory memory,
      final long position,
      final long maxSize,
      final List<ByteBuffer> byteBuffers
  )
  {
    long written = 0;

    for (final ByteBuffer utf8Datum : byteBuffers) {
      final int len = utf8Datum.remaining();

      if (written + ROW_OVERHEAD_BYTES > maxSize) {
        return -1;
      }

      if (len == 1 && utf8Datum.get(utf8Datum.position()) == FrameWriterUtils.NULL_STRING_MARKER) {
        // Null.
        memory.putByte(position + written, NULL_BYTE);
        written++;
      } else {
        // Not null.
        if (written + len + ROW_OVERHEAD_BYTES > maxSize) {
          return -1;
        }

        memory.putByte(position + written, NOT_NULL_BYTE);
        written++;

        if (len > 0) {
          FrameWriterUtils.copyByteBufferToMemory(utf8Datum, memory, position + written, len, false);
          written += len;
        }
      }

      memory.putByte(position + written, VALUE_TERMINATOR);
      written++;
    }

    if (written + 1 > maxSize) {
      return -1;
    }

    memory.putByte(position + written, ROW_TERMINATOR);
    written++;

    return written;
  }
}
