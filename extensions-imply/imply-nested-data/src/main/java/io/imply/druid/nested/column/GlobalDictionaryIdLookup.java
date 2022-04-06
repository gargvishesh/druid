/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.Double2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2LongLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import javax.annotation.Nullable;

/**
 * Ingestion time dictionary identifier lookup, used by {@link NestedDataColumnSerializer} to build a global dictionary
 * id to value mapping for the 'stacked' global value dictionaries. Also provides reverse lookup for numeric values,
 * since they serialize a value column
 */
public class GlobalDictionaryIdLookup
{
  private final Object2IntMap<String> stringLookup;

  private final Long2IntMap longLookup;
  private final Int2LongMap reverseLongLookup;

  private final Double2IntMap doubleLookup;
  private final Int2DoubleMap reverseDoubleLookup;

  private int dictionarySize;

  public GlobalDictionaryIdLookup()
  {
    this.stringLookup = new Object2IntLinkedOpenHashMap<>();
    this.longLookup = new Long2IntLinkedOpenHashMap();
    this.reverseLongLookup = new Int2LongLinkedOpenHashMap();
    this.doubleLookup = new Double2IntLinkedOpenHashMap();
    this.reverseDoubleLookup = new Int2DoubleLinkedOpenHashMap();
  }

  void addString(@Nullable String value)
  {
    Preconditions.checkState(
        longLookup.size() == 0 && doubleLookup.size() == 0,
        "All string values must be inserted to the lookup before long and double types"
    );
    int id = dictionarySize++;
    stringLookup.put(value, id);
  }

  int lookupString(@Nullable String value)
  {
    return stringLookup.getInt(value);
  }

  void addLong(long value)
  {
    Preconditions.checkState(
        doubleLookup.size() == 0,
        "All long values must be inserted to the lookup before double types"
    );
    int id = dictionarySize++;
    longLookup.put(value, id);
    reverseLongLookup.put(id, value);
  }

  int lookupLong(@Nullable Long value)
  {
    if (value == null) {
      return 0;
    }
    return longLookup.get(value.longValue());
  }

  @Nullable Long lookupLong(int id)
  {
    if (id == 0) {
      return null;
    }
    return reverseLongLookup.get(id);
  }

  void addDouble(double value)
  {
    int id = dictionarySize++;
    doubleLookup.put(value, id);
    reverseDoubleLookup.put(id, value);
  }

  int lookupDouble(@Nullable Double value)
  {
    if (value == null) {
      return 0;
    }
    return doubleLookup.get(value.doubleValue());
  }

  @Nullable Double lookupDouble(int id)
  {
    if (id == 0) {
      return null;
    }
    return reverseDoubleLookup.get(id);
  }
}
