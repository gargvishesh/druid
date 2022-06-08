/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.WritableMemory;

import java.io.Closeable;
import java.nio.ByteOrder;

public interface FrameWriter extends Closeable
{
  /**
   * Write the current row to the frame that is under construction, if there is enough space to do so.
   *
   * If this method returns false on an empty frame, or in a situation where starting a new frame is impractical,
   * it is conventional (although not required) for the caller to throw
   * {@link io.imply.druid.talaria.frame.processor.FrameRowTooLargeException}.
   *
   * @return true if the row was written, false if there was not enough space
   */
  boolean addSelection();

  /**
   * Returns the number of rows written so far.
   */
  int getNumRows();

  /**
   * Returns the number of bytes that would be written by {@link #writeTo} if called now.
   */
  long getTotalSize();

  /**
   * Writes the frame to the provided memory location, which must have at least {@link #getTotalSize()} bytes available.
   * Once this method is called, the frame writer is no longer usable and must be closed.
   *
   * Returns the number of bytes written, which will equal {@link #getTotalSize()}.
   *
   * @throws IllegalStateException if the provided memory does not have sufficient space to write this frame.
   */
  long writeTo(WritableMemory memory, long position);

  /**
   * Returns a frame as a newly-allocated byte array.
   *
   * In general, it is more efficient to write a frame into a pre-existing buffer, but this method is provided
   * as a convenience.
   *
   * Once this method is called, the frame writer is no longer usable and must be closed.
   */
  default byte[] toByteArray()
  {
    // Frame sizes are expected to be much smaller than the max value of an int; do a checked cast here
    // to validate.
    final byte[] bytes = new byte[Ints.checkedCast(getTotalSize())];
    writeTo(WritableMemory.writableWrap(bytes, ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }

  @Override
  void close();
}
