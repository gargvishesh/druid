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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class LocalDimensionDictionary
{
  private final Int2IntOpenHashMap globalIdToLocalId = new Int2IntOpenHashMap();

  private final IntList localIdToGlobalId = new IntArrayList();

  public LocalDimensionDictionary()
  {
    this.globalIdToLocalId.defaultReturnValue(-1);
  }

  public Int2IntOpenHashMap getGlobalIdToLocalId()
  {
    return globalIdToLocalId;
  }

  public IntList getLocalIdToGlobalId()
  {
    return localIdToGlobalId;
  }

  public int add(int originalValue)
  {
    int prev = globalIdToLocalId.get(originalValue);
    if (prev >= 0) {
      return prev;
    }
    final int index = localIdToGlobalId.size();
    globalIdToLocalId.put(originalValue, index);
    localIdToGlobalId.add(originalValue);

    return index;
  }

  public int size()
  {
    return localIdToGlobalId.size();
  }
}
