/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;

import java.util.Optional;

/**
 * Allocator of WritableMemory. Not thread safe.
 */
public interface MemoryAllocator
{
  /**
   * Allocates a block of memory of capacity {@param size}. Returns empty if no more memory is available.
   * The memory can be freed by closing the returned {@link ResourceHolder}.
   *
   * The returned WritableMemory object will use little-endian byte order.
   */
  Optional<ResourceHolder<WritableMemory>> allocate(long size);

  /**
   * Returns the number of bytes available for allocations.
   *
   * May return {@link Long#MAX_VALUE} if there is no inherent limit. This generally does not mean you can actually
   * allocate 9 exabytes.
   */
  long available();
}
