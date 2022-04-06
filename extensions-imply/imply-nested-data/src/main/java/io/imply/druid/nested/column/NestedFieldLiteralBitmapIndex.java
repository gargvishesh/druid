/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * {@link BitmapIndex} to support fast filtering on {@link NestedFieldLiteralDictionaryEncodedColumn}
 */
public class NestedFieldLiteralBitmapIndex implements BitmapIndex
{
  private final NestedLiteralTypeInfo.TypeSet types;
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final FixedIndexed<Integer> dictionary;
  private final GenericIndexed<String> globalDictionary;
  private final FixedIndexed<Long> globalLongDictionary;
  private final FixedIndexed<Double> globalDoubleDictionary;
  @Nullable
  private final ColumnType singleType;
  private final int adjustLongId;
  private final int adjustDoubleId;

  public NestedFieldLiteralBitmapIndex(
      NestedLiteralTypeInfo.TypeSet types,
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      FixedIndexed<Integer> dictionary,
      GenericIndexed<String> globalDictionary,
      FixedIndexed<Long> globalLongDictionary,
      FixedIndexed<Double> globalDoubleDictionary
  )
  {
    this.types = types;
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
    this.globalDictionary = globalDictionary;
    this.globalLongDictionary = globalLongDictionary;
    this.globalDoubleDictionary = globalDoubleDictionary;
    this.singleType = types.getSingleType();
    this.adjustLongId = globalDictionary.size();
    this.adjustDoubleId = adjustLongId + globalLongDictionary.size();
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
    int globalId = dictionary.get(index);
    if (globalId < adjustLongId) {
      return globalDictionary.get(globalId);
    } else if (globalId < adjustDoubleId) {
      return String.valueOf(globalLongDictionary.get(globalId - adjustLongId));
    } else {
      return String.valueOf(globalDoubleDictionary.get(globalId - adjustDoubleId));
    }
  }

  @Override
  public boolean hasNulls()
  {
    return dictionary.get(0) == 0;
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

    if (singleType != null) {
      switch (singleType.getType()) {
        case STRING:
          return dictionary.indexOf(globalDictionary.indexOf(value));
        case LONG:
          Long someLong = Longs.tryParse(value);
          if (someLong != null) {
            final int globalId = globalLongDictionary.indexOf(someLong);
            return globalId < 0 ? globalId - adjustLongId : dictionary.indexOf(globalId + adjustLongId);
          }
          return dictionary.indexOf(0);
        case DOUBLE:
          Double someDouble = Doubles.tryParse(value);
          if (someDouble != null) {
            final int globalId = globalDoubleDictionary.indexOf(Doubles.tryParse(value));
            return globalId < 0 ? globalId - adjustDoubleId : dictionary.indexOf(globalId + adjustDoubleId);
          }
          return dictionary.indexOf(0);
        default:
          throw new IAE("Unsupported nested type [%s]", singleType);
      }
    }
    // multi-type, return first dictionary that matches
    int globalId = globalDictionary.indexOf(value);
    int adjust = 0;
    if (globalId < 0) {
      Long someLong = Longs.tryParse(value);
      if (someLong != null) {
        globalId = globalLongDictionary.indexOf(someLong);
        adjust = adjustLongId;
      }
    }
    if (globalId < 0) {
      Double someDouble = Doubles.tryParse(value);
      if (someDouble != null) {
        globalId = globalDoubleDictionary.indexOf(someDouble);
        adjust = adjustDoubleId;
      }
    }
    return globalId < 0 ? globalId : dictionary.indexOf(globalId + adjust);
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
      boolean endStrict,
      Predicate<String> matcher
  )
  {
    final IntIntPair[] theRanges = getIndexRanges(startValue, startStrict, endValue, endStrict, matcher);
    return () -> new Iterator<ImmutableBitmap>()
    {
      final IntIntPair[] ranges = theRanges;
      int currRange = 0;
      int currIndex = ranges[0].leftInt();
      int currEnd = ranges[0].rightInt();
      int found;
      {
        found = findNext();
      }

      private int findNext()
      {
        while (currIndex < currEnd && !matcher.test(getValue(currIndex))) {
          currIndex++;
        }

        if (currIndex < currEnd) {
          return currIndex++;
        } else if (++currRange < ranges.length) {
          currIndex = ranges[currRange].leftInt();
          currEnd = ranges[currRange].rightInt();
          return findNext();
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

  private IntIntPair[] getIndexRanges(String startValue, boolean startStrict, String endValue, boolean endStrict, Predicate<String> matcher)
  {
    int stringRangeEnd = dictionary.indexOf(adjustLongId);
    int longRangeEnd = dictionary.indexOf(adjustDoubleId);
    if (stringRangeEnd < 0) {
      stringRangeEnd = -(stringRangeEnd + 1);
    }
    if (longRangeEnd < 0) {
      longRangeEnd = -(longRangeEnd + 1);
    }

    final IntIntPair stringsRange = getRange(
        startValue,
        startStrict,
        endValue,
        endStrict,
        0,
        stringRangeEnd,
        globalDictionary::indexOf
    );
    final IntIntPair longsRange = getRange(
        startValue,
        startStrict,
        endValue,
        endStrict,
        stringRangeEnd,
        longRangeEnd,
        (s) -> {
          Long theLong = Longs.tryParse(s);
          int id = globalLongDictionary.indexOf(theLong);
          if (id < 0) {
            return (-(id + 1)) + adjustLongId;
          }
          return id + adjustLongId;
        }
    );
    final IntIntPair doublesRange = getRange(
        startValue,
        startStrict,
        endValue,
        endStrict,
        longRangeEnd,
        dictionary.size(),
        (s) -> {
          Double theDouble = Doubles.tryParse(s);
          int id = globalDoubleDictionary.indexOf(theDouble);
          if (id < 0) {
            return (-(id + 1)) + adjustDoubleId;
          }
          return id + adjustDoubleId;
        }
    );
    return new IntIntPair[]{stringsRange, longsRange, doublesRange};
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
          next = getIndex(nextValue);
        }
      }
    };
  }

  private IntIntPair getRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      int rangeStart,
      int rangeEnd,
      GlobalIndexGetFunction getFn
  )
  {
    int startIndex, endIndex;
    if (startValue == null) {
      startIndex = rangeStart;
    } else {
      final int found = dictionary.indexOf(getFn.indexOf(startValue));
      if (found >= 0) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found);
      }
    }

    if (endValue == null) {
      endIndex = rangeEnd;
    } else {
      final int found = dictionary.indexOf(getFn.indexOf(endValue));
      if (found >= 0) {
        endIndex = endStrict ? found : found + 1;
      } else {
        endIndex = -(found);
      }
    }

    endIndex = Math.max(startIndex, endIndex);
    return new IntIntImmutablePair(Math.max(Math.min(startIndex, rangeEnd), rangeStart), Math.min(endIndex, rangeEnd));
  }

  @FunctionalInterface
  interface GlobalIndexGetFunction
  {
    int indexOf(String value);
  }
}
