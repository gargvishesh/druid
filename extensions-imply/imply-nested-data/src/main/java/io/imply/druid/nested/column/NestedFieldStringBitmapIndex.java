/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

public class NestedFieldStringBitmapIndex implements BitmapIndex
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<Integer> dictionary;
  private final GenericIndexed<String> globalDictionary;

  public NestedFieldStringBitmapIndex(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      GenericIndexed<Integer> dictionary,
      GenericIndexed<String> globalDictionary
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
    this.globalDictionary = globalDictionary;
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  @Nullable
  @Override
  public String getValue(int index)
  {
    return globalDictionary.get(dictionary.get(index));
  }

  @Override
  public boolean hasNulls()
  {
    return dictionary.indexOf(0) >= 0;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public int getIndex(@Nullable String value)
  {
    if (value == null) {
      return dictionary.indexOf(0);
    }
    return dictionary.indexOf(globalDictionary.indexOf(value));
  }

  @Override
  public ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  @Override
  public ImmutableBitmap getBitmapForValue(@Nullable String value)
  {
    return getBitmap(getIndex(value));
  }

  @Override
  public Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  )
  {
    final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
    final int start = range.leftInt(), end = range.rightInt();
    return () -> new Iterator<ImmutableBitmap>()
    {
      final IntIterator rangeIterator = IntListUtils.fromTo(start, end).iterator();

      @Override
      public boolean hasNext()
      {
        return rangeIterator.hasNext();
      }

      @Override
      public ImmutableBitmap next()
      {
        return getBitmap(rangeIterator.nextInt());
      }
    };
  }

  @Override
  public Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      Predicate<String> indexMatcher
  )
  {
    final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
    final int start = range.leftInt(), end = range.rightInt();
    return () -> new Iterator<ImmutableBitmap>()
    {
      int currIndex = start;
      int found;
      {
        found = findNext();
      }

      private int findNext()
      {
        while (currIndex < end && !indexMatcher.test(globalDictionary.get(dictionary.get(currIndex)))) {
          currIndex++;
        }

        if (currIndex < end) {
          return currIndex++;
        } else {
          return -1;
        }
      }

      @Override
      public boolean hasNext()
      {
        return found != -1;
      }

      @Override
      public ImmutableBitmap next()
      {
        int cur = found;

        if (cur == -1) {
          throw new NoSuchElementException();
        }

        found = findNext();
        return getBitmap(cur);
      }
    };
  }

  @Override
  public Iterable<ImmutableBitmap> getBitmapsForValues(Set<String> values)
  {
    return () -> new Iterator<ImmutableBitmap>()
    {
      final Iterator<String> iterator = values.iterator();
      int next = -1;

      @Override
      public boolean hasNext()
      {
        if (next < 0) {
          findNext();
        }
        return next >= 0;
      }

      @Override
      public ImmutableBitmap next()
      {
        if (next < 0) {
          findNext();
          if (next < 0) {
            throw new NoSuchElementException();
          }
        }
        final int swap = next;
        next = -1;
        return getBitmap(swap);
      }

      private void findNext()
      {
        while (next < 0 && iterator.hasNext()) {
          String nextValue = iterator.next();
          next = dictionary.indexOf(globalDictionary.indexOf(nextValue));
        }
      }
    };
  }

  private IntIntPair getRange(@Nullable String startValue, boolean startStrict, @Nullable String endValue, boolean endStrict)
  {
    int startIndex, endIndex;
    if (startValue == null) {
      startIndex = 0;
    } else {
      final int found = dictionary.indexOf(globalDictionary.indexOf(NullHandling.emptyToNullIfNeeded(startValue)));
      if (found >= 0) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (endValue == null) {
      endIndex = dictionary.size();
    } else {
      final int found = dictionary.indexOf(globalDictionary.indexOf(NullHandling.emptyToNullIfNeeded(endValue)));
      if (found >= 0) {
        endIndex = endStrict ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }

    endIndex = Math.max(startIndex, endIndex);
    return new IntIntImmutablePair(startIndex, endIndex);
  }
}
