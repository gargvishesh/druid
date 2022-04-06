/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;

import java.util.Comparator;
import java.util.List;

/**
 * {@link Comparator} based {@link org.apache.druid.segment.SortedDimensionDictionary}
 *
 * There are a number of unused methods, because nested columns don't merge bitmap indexes during
 * the merge phase, rather they are created when serializing the column, but leaving for now for
 * compatibility with the other implementation
 */
public class ComparatorSortedDimensionDictionary<T>
{
  private final List<T> sortedVals;
  private final int[] idToIndex;
  private final int[] indexToId;

  public ComparatorSortedDimensionDictionary(List<T> idToValue, Comparator<T> comparator, int length)
  {
    Object2IntSortedMap<T> sortedMap = new Object2IntRBTreeMap<>(comparator);
    for (int id = 0; id < length; id++) {
      T value = idToValue.get(id);
      sortedMap.put(value, id);
    }
    this.sortedVals = Lists.newArrayList(sortedMap.keySet());
    this.idToIndex = new int[length];
    this.indexToId = new int[length];
    int index = 0;
    for (IntIterator iterator = sortedMap.values().iterator(); iterator.hasNext(); ) {
      int id = iterator.nextInt();
      idToIndex[id] = index;
      indexToId[index] = id;
      index++;
    }
  }

  public int getUnsortedIdFromSortedId(int index)
  {
    return indexToId[index];
  }

  public int getSortedIdFromUnsortedId(int id)
  {
    return idToIndex[id];
  }

  public T getValueFromSortedId(int index)
  {
    return sortedVals.get(index);
  }
}
