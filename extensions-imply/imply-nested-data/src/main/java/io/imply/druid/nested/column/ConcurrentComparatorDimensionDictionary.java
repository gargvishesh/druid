/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe version of {@link ComparatorDimensionDictionary}, equivalent to
 * {@link org.apache.druid.segment.DimensionDictionary} (which is also thread-safe).
 *
 * This is not really used right now since realtime queries do not use the dictionaries
 */
public class ConcurrentComparatorDimensionDictionary<T> extends ComparatorDimensionDictionary<T>
{
  private final ReentrantReadWriteLock lock;

  public ConcurrentComparatorDimensionDictionary(Comparator<T> comparator)
  {
    super(comparator);
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  public int getId(@Nullable T value)
  {
    lock.readLock().lock();
    try {
      return super.getId(value);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  @Nullable
  public T getValue(int id)
  {
    lock.readLock().lock();
    try {
      return super.getValue(id);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int size()
  {
    lock.readLock().lock();
    try {
      return super.size();
    }
    finally {
      lock.readLock().unlock();
    }
  }


  @Override
  public int add(@Nullable T originalValue)
  {
    lock.writeLock().lock();
    try {
      return super.add(originalValue);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public T getMinValue()
  {
    lock.readLock().lock();
    try {
      return super.getMinValue();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public T getMaxValue()
  {
    lock.readLock().lock();
    try {
      return super.getMaxValue();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public ComparatorSortedDimensionDictionary<T> sort()
  {
    lock.readLock().lock();
    try {
      return super.sort();
    }
    finally {
      lock.readLock().unlock();
    }
  }
}
