/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

public class SplitUtils
{

  /**
   * Creates "numSplits" lists from "iterator", trying to keep each one as evenly-sized as possible. Some lists may
   * be empty.
   *
   * An important note: the items are assigned round-robin. This means that any useful locality within the iterator
   * will not be retained. In may be nice to change this in the future.
   */
  public static <T> List<List<T>> makeSplits(final Iterator<T> iterator, final int numSplits)
  {
    final List<List<T>> splitsList = new ArrayList<>(numSplits);

    while (splitsList.size() < numSplits) {
      splitsList.add(new ArrayList<>());
    }

    int i = 0;
    while (iterator.hasNext()) {
      final T obj = iterator.next();
      splitsList.get(i % numSplits).add(obj);
      i++;
    }

    return splitsList;
  }

  /**
   * Creates "numSplits" lists from "iterator", trying to keep each one as evenly-weighted as possible. Some lists may
   * be empty.
   *
   * An important note: each item is assigned to the split list that has the lowest weight at the time that item is
   * encountered, which leads to pseudo-round-robin assignment. This means that any useful locality within the iterator
   * will not be retained. It may be nice to change this in the future.
   */
  public static <T> List<List<T>> makeSplits(
      final Iterator<T> iterator,
      final ToLongFunction<T> weightFunction,
      final int numSplits
  )
  {
    final List<List<T>> splitsList = new ArrayList<>(numSplits);
    final PriorityQueue<ListWithWeight<T>> pq = new PriorityQueue<>(
        numSplits,
        Comparator.comparing(ListWithWeight::getWeight)
    );

    while (splitsList.size() < numSplits) {
      final ArrayList<T> list = new ArrayList<>();
      pq.add(new ListWithWeight<>(list, 0));
      splitsList.add(list);
    }

    while (iterator.hasNext()) {
      final T obj = iterator.next();
      final long itemWeight = weightFunction.applyAsLong(obj);
      final ListWithWeight<T> listWithWeight = pq.remove();
      listWithWeight.getList().add(obj);
      pq.add(new ListWithWeight<>(listWithWeight.getList(), listWithWeight.getWeight() + itemWeight));
    }

    return splitsList;
  }

  private static class ListWithWeight<T>
  {
    private final List<T> list;
    private final long totalSize;

    public ListWithWeight(List<T> list, long totalSize)
    {
      this.list = list;
      this.totalSize = totalSize;
    }

    public List<T> getList()
    {
      return list;
    }

    public long getWeight()
    {
      return totalSize;
    }
  }
}
