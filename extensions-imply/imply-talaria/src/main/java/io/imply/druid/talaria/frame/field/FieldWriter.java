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

import java.io.Closeable;

/**
 * Helper used to write field values to row-based frames or {@link io.imply.druid.talaria.frame.cluster.ClusterByKey}.
 *
 * Not thread-safe.
 */
public interface FieldWriter extends Closeable
{
  /**
   * Writes the current selection at the given memory position.
   *
   * @param memory   memory region in little-endian order
   * @param position position to write
   * @param maxSize  maximum number of bytes to write
   *
   * @return number of bytes written, or -1 if "maxSize" was not enough memory
   */
  long writeTo(WritableMemory memory, long position, long maxSize);

  /**
   * Releases resources held by this writer.
   */
  @Override
  void close();
}
