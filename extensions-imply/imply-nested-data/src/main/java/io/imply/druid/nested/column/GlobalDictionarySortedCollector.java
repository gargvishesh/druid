/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.segment.data.Indexed;

/**
 * Container to collect a set of sorted {@link Indexed} representing the global value dictionaries of some
 * {@link NestedDataComplexColumn}, to later use with {@link org.apache.druid.segment.DictionaryMergingIterator}
 * to merge into a new global dictionary
 */
public class GlobalDictionarySortedCollector
{
  private final Indexed<String> sortedStrings;
  private final Indexed<Long> sortedLongs;
  private final Indexed<Double> sortedDoubles;

  public GlobalDictionarySortedCollector(
      Indexed<String> sortedStrings,
      Indexed<Long> sortedLongs,
      Indexed<Double> sortedDoubles
  )
  {
    this.sortedStrings = sortedStrings;
    this.sortedLongs = sortedLongs;
    this.sortedDoubles = sortedDoubles;
  }

  public Indexed<String> getSortedStrings()
  {
    return sortedStrings;
  }

  public Indexed<Long> getSortedLongs()
  {
    return sortedLongs;
  }

  public Indexed<Double> getSortedDoubles()
  {
    return sortedDoubles;
  }

  public int size()
  {
    return sortedStrings.size() + sortedLongs.size() + sortedDoubles.size();
  }
}
