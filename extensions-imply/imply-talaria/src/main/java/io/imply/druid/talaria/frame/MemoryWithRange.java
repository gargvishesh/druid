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

public class MemoryWithRange<T extends Memory>
{
  private final T memory;
  private final long start;
  private final long end;

  public MemoryWithRange(T memory, long start, long end)
  {
    this.memory = memory;
    this.start = start;
    this.end = end;
  }

  public T memory()
  {
    return memory;
  }

  public long start()
  {
    return start;
  }

  public long end()
  {
    return end;
  }

  public long length()
  {
    return end - start;
  }
}
