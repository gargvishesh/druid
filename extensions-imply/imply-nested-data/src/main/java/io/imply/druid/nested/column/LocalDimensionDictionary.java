/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class LocalDimensionDictionary
{
  private final Int2IntOpenHashMap globalIdToLocalId = new Int2IntOpenHashMap();

  public LocalDimensionDictionary()
  {
    this.globalIdToLocalId.defaultReturnValue(-1);
  }

  public Int2IntOpenHashMap getGlobalIdToLocalId()
  {
    return globalIdToLocalId;
  }
  
  private int nextLocalId = 0;
  
  public int add(int originalValue)
  {
    int prev = globalIdToLocalId.get(originalValue);
    if (prev >= 0) {
      return prev;
    }
    final int index = nextLocalId++;
    globalIdToLocalId.put(originalValue, index);

    return index;
  }

  public int size()
  {
    return nextLocalId;
  }
}
