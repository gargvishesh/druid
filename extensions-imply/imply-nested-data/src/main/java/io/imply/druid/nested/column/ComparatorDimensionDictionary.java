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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link Comparator} based {@link org.apache.druid.segment.DimensionDictionary}
 *
 * there are a lot of unused methods in here for now since the only thing this is used for is to build up
 * the unsorted dictionary and then it is converted to a {@link ComparatorSortedDimensionDictionary}, but
 * leaving the unused methods in place for now to be basically compatible with the other implementation.
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
  private final ReentrantReadWriteLock lock;

  private final Comparator<T> comparator;

  public ComparatorDimensionDictionary(Comparator<T> comparator)
  {
    this.lock = new ReentrantReadWriteLock();
    this.comparator = comparator;
    valueToId.defaultReturnValue(ABSENT_VALUE_ID);
  }

  public int getId(@Nullable T value)
  {
    lock.readLock().lock();
    try {
      if (value == null) {
        return idForNull;
      }
      return valueToId.getInt(value);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Nullable
  public T getValue(int id)
  {
    lock.readLock().lock();
    try {
      if (id == idForNull) {
        return null;
      }
      return idToValue.get(id);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public int size()
  {
    lock.readLock().lock();
    try {
      // using idToValue rather than valueToId because the valueToId doesn't account null value, if it is present.
      return idToValue.size();
    }
    finally {
      lock.readLock().unlock();
    }
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
    lock.writeLock().lock();
    try {
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
    finally {
      lock.writeLock().unlock();
    }
  }

  public T getMinValue()
  {
    lock.readLock().lock();
    try {
      return minValue;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public T getMaxValue()
  {
    lock.readLock().lock();
    try {
      return maxValue;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public int getIdForNull()
  {
    return idForNull;
  }

  public ComparatorSortedDimensionDictionary<T> sort()
  {
    lock.readLock().lock();
    try {
      return new ComparatorSortedDimensionDictionary<T>(idToValue, comparator, idToValue.size());
    }
    finally {
      lock.readLock().unlock();
    }
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
