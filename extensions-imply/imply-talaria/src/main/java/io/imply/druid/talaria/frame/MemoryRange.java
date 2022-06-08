/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import org.apache.datasketches.memory.Memory;

/**
 * Reference to a particular region of some {@link Memory}. This is used because it is cheaper to create than
 * calling {@link Memory#region}.
 *
 * Not immutable. The pointed-to range may change as this object gets reused.
 */
public class MemoryRange<T extends Memory>
{
  private T memory;
  private long start;
  private long length;

  public MemoryRange(T memory, long start, long length)
  {
    set(memory, start, length);
  }

  /**
   * Returns the underlying memory *without* clipping it to this particular range. Callers must remember to continue
   * applying the offset given by {@link #start} and capacity given by {@link #length}.
   */
  public T memory()
  {
    return memory;
  }

  public long start()
  {
    return start;
  }

  public long length()
  {
    return length;
  }

  public void set(final T memory, final long start, final long length)
  {
    this.memory = memory;
    this.start = start;
    this.length = length;
  }
}
