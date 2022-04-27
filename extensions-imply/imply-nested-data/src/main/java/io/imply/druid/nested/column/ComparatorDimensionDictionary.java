/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Comparator} based {@link org.apache.druid.segment.DimensionDictionary}
 *
 * there are a lot of unused methods in here for now since the only thing this is used for is to build up
 * the unsorted dictionary and then it is converted to a {@link ComparatorSortedDimensionDictionary}, but
 * leaving the unused methods in place for now to be basically compatible with the other implementation.
 *
 * This version is not thread-safe since we currently don't use the dictionary for realtime queries, if this changes
 * we need to use {@link ConcurrentComparatorDimensionDictionary} instead.
 */
public class ComparatorDimensionDictionary<T>
{
  public static final int ABSENT_VALUE_ID = -1;

  @Nullable
  private T minValue = null;
  @Nullable
  private T maxValue = null;
  private volatile int idForNull = ABSENT_VALUE_ID;

  private final AtomicLong sizeInBytes = new AtomicLong(0L);
  private final Object2IntMap<T> valueToId = new Object2IntOpenHashMap<>();

  private final List<T> idToValue = new ArrayList<>();
  private final Comparator<T> comparator;

  public ComparatorDimensionDictionary(Comparator<T> comparator)
  {
    this.comparator = comparator;
    valueToId.defaultReturnValue(ABSENT_VALUE_ID);
  }

  public int getId(@Nullable T value)
  {
    if (value == null) {
      return idForNull;
    }
    return valueToId.getInt(value);
  }

  @Nullable
  public T getValue(int id)
  {
    if (id == idForNull) {
      return null;
    }
    return idToValue.get(id);
  }

  public int size()
  {
    // using idToValue rather than valueToId because the valueToId doesn't account null value, if it is present.
    return idToValue.size();
  }

  /**
   * Gets the current size of this dictionary in bytes.
   *
   * @throws IllegalStateException if size computation is disabled.
   */
  public long sizeInBytes()
  {
    return sizeInBytes.get();
  }

  public int add(@Nullable T originalValue)
  {
    if (originalValue == null) {
      if (idForNull == ABSENT_VALUE_ID) {
        idForNull = idToValue.size();
        idToValue.add(null);
      }
      return idForNull;
    }
    int prev = valueToId.getInt(originalValue);
    if (prev >= 0) {
      return prev;
    }
    final int index = idToValue.size();
    valueToId.put(originalValue, index);
    idToValue.add(originalValue);

    // Add size of new dim value and 2 references (valueToId and idToValue)
    sizeInBytes.addAndGet(estimateSizeOfValue(originalValue) + (2L * Long.BYTES));

    minValue = minValue == null || comparator.compare(minValue, originalValue) > 0 ? originalValue : minValue;
    maxValue = maxValue == null || comparator.compare(maxValue, originalValue) < 0 ? originalValue : maxValue;
    return index;
  }

  public T getMinValue()
  {
    return minValue;
  }

  public T getMaxValue()
  {
    return maxValue;
  }

  public int getIdForNull()
  {
    return idForNull;
  }

  public ComparatorSortedDimensionDictionary<T> sort()
  {
    return new ComparatorSortedDimensionDictionary<T>(idToValue, comparator, idToValue.size());
  }

  /**
   * Estimates the size of the dimension value in bytes. This method is called
   * only when a new dimension value is being added to the lookup.
   *
   * @throws UnsupportedOperationException Implementations that want to estimate
   *                                       memory must override this method.
   */
  public long estimateSizeOfValue(T value)
  {
    throw new UnsupportedOperationException();
  }
}
