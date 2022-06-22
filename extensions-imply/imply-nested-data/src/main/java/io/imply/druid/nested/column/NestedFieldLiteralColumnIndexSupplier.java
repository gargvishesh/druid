/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.base.Predicate;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.doubles.DoubleArraySet;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleBitmapColumnIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

// todo: break this up, is a beast
public class NestedFieldLiteralColumnIndexSupplier implements ColumnIndexSupplier
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

  public NestedFieldLiteralColumnIndexSupplier(
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

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      // null index is always 0 in the global dictionary, even if there are no null rows in any of the literal columns
      return (T) (NullValueIndex) () -> new SimpleImmutableBitmapIndex(bitmaps.get(0));
    }

    if (singleType != null) {
      switch (singleType.getType()) {
        case STRING:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedStringLiteralValueSetIndex();
          } else if (clazz.equals(LexicographicalRangeIndex.class)) {
            return (T) new NestedStringLiteralLexicographicalRangeIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedStringLiteralPredicateIndex();
          }
          return null;
        case LONG:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedLongLiteralValueSetIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedLongLiteralPredicateIndex();
          }
          return null;
        case DOUBLE:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedDoubleLiteralValueSetIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedDoubleLiteralPredicateIndex();
          }
          return null;
        default:
          return null;
      }
    }
    if (clazz.equals(StringValueSetIndex.class)) {
      return (T) new NestedAnyLiteralValueSetIndex();
    } else if (clazz.equals(DruidPredicateIndex.class)) {
      return (T) new NestedAnyLiteralPredicateIndex();
    }
    return null;
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  private IntIntPair getGlobalRange(
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
      final int found = getFn.indexOf(startValue);
      if (found >= 0) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (endValue == null) {
      endIndex = rangeEnd;
    } else {
      final int found = getFn.indexOf(endValue);
      if (found >= 0) {
        endIndex = endStrict ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }

    endIndex = Math.max(startIndex, endIndex);
    return new IntIntImmutablePair(startIndex, endIndex);
  }

  @FunctionalInterface
  interface GlobalIndexGetFunction
  {
    int indexOf(String value);
  }

  private class NestedStringLiteralValueSetIndex implements StringValueSetIndex
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return (double) getBitmap(dictionary.indexOf(globalDictionary.indexOf(value))).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.wrapDimensionValue(getBitmap(dictionary.indexOf(globalDictionary.indexOf(value))));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
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
      };
    }
  }

  private class NestedStringLiteralLexicographicalRangeIndex implements LexicographicalRangeIndex
  {

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          final IntIntPair range = getGlobalRange(
              startValue,
              startStrict,
              endValue,
              endStrict,
              0,
              globalDictionary.size(),
              globalDictionary::indexOf
          );
          final int start = range.leftInt(), end = range.rightInt();
          // iterates over the range of values in the global dictionary, mapping to relevant range in the local
          // dictionary, skipping duplicates
          return () -> new Iterator<ImmutableBitmap>()
          {
            int currentGlobalIndex = start;
            // initialize to -1 because findNext uses this field to check for duplicates, and could legitimately find
            // 0 for the first candidate
            @SuppressWarnings("UnusedAssignment")
            int currentLocalIndex = -1;
            {
              currentLocalIndex = findNext();
            }

            private int findNext()
            {
              int candidateLocalIndex = Math.abs(dictionary.indexOf(currentGlobalIndex));
              while (currentGlobalIndex < end && candidateLocalIndex == currentLocalIndex) {
                currentGlobalIndex++;
                candidateLocalIndex = Math.abs(dictionary.indexOf(currentGlobalIndex));
              }
              if (currentGlobalIndex < end) {
                currentGlobalIndex++;
                return candidateLocalIndex;
              } else {
                return -1;
              }
            }

            @Override
            public boolean hasNext()
            {
              return currentLocalIndex != -1;
            }

            @Override
            public ImmutableBitmap next()
            {
              int cur = currentLocalIndex;

              if (cur == -1) {
                throw new NoSuchElementException();
              }

              currentLocalIndex = findNext();
              return getBitmap(cur);
            }
          };
        }
      };
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        Predicate<String> matcher
    )
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          final IntIntPair stringsRange = getGlobalRange(
              startValue,
              startStrict,
              endValue,
              endStrict,
              0,
              globalDictionary.size(),
              globalDictionary::indexOf
          );
          // iterates over the range of values in the global dictionary, mapping to relevant range in the local
          // dictionary, skipping duplicates
          return () -> new Iterator<ImmutableBitmap>()
          {
            int currentGlobalIndex = stringsRange.leftInt();
            final int end = stringsRange.rightInt();
            // initialize to -1 because findNext uses this field to check for duplicates, and could legitimately find
            // 0 for the first candidate
            @SuppressWarnings("UnusedAssignment")
            int currentLocalIndex = -1;
            {
              currentLocalIndex = findNext();
            }

            private int findNext()
            {
              int candidateLocalIndex = Math.abs(dictionary.indexOf(currentGlobalIndex));
              while (currentGlobalIndex < end && shouldSkipGlobal(candidateLocalIndex)) {
                currentGlobalIndex++;
                candidateLocalIndex = Math.abs(dictionary.indexOf(currentGlobalIndex));
              }

              if (currentGlobalIndex < end) {
                currentGlobalIndex++;
                return candidateLocalIndex;
              } else {
                return -1;
              }
            }

            private boolean shouldSkipGlobal(int candidate)
            {
              return currentLocalIndex == candidate || !matcher.apply(globalDictionary.get(currentGlobalIndex));
            }


            @Override
            public boolean hasNext()
            {
              return currentLocalIndex != -1;
            }

            @Override
            public ImmutableBitmap next()
            {
              int cur = currentLocalIndex;

              if (cur == -1) {
                throw new NoSuchElementException();
              }

              currentLocalIndex = findNext();
              return getBitmap(cur);
            }
          };
        }
      };
    }
  }

  private class NestedStringLiteralPredicateIndex implements DruidPredicateIndex
  {
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {

          return () -> new Iterator<ImmutableBitmap>()
          {
            final Predicate<String> stringPredicate = matcherFactory.makeStringPredicate();

            // todo: int iterator
            final Iterator<Integer> iterator = dictionary.iterator();
            int next;
            int index = 0;
            boolean nextSet = false;

            @Override
            public boolean hasNext()
            {
              if (!nextSet) {
                findNext();
              }
              return nextSet;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (!nextSet) {
                findNext();
                if (!nextSet) {
                  throw new NoSuchElementException();
                }
              }
              nextSet = false;
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                nextSet = stringPredicate.apply(globalDictionary.get(nextValue));
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }


  private class NestedLongLiteralValueSetIndex implements StringValueSetIndex
  {

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final Long longValue = Longs.tryParse(value);
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          if (longValue == null) {
            return (double) getBitmap(dictionary.indexOf(0)).size() / totalRows;
          }
          return (double) getBitmap(dictionary.indexOf(globalLongDictionary.indexOf(longValue) + adjustLongId)).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          if (longValue == null) {
            return bitmapResultFactory.wrapDimensionValue(getBitmap(dictionary.indexOf(0)));
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(dictionary.indexOf(globalLongDictionary.indexOf(longValue) + adjustLongId)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          LongSet longs = new LongArraySet(values.size());
          for (String value : values) {
            Long theValue = Longs.tryParse(value);
            if (theValue != null) {
              longs.add(theValue.longValue());
            }
          }
          return () -> new Iterator<ImmutableBitmap>()
          {
            final LongIterator iterator = longs.iterator();
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
                long nextValue = iterator.nextLong();
                next = dictionary.indexOf(globalLongDictionary.indexOf(nextValue) + adjustLongId);
              }
            }
          };
        }
      };
    }
  }

  private class NestedLongLiteralPredicateIndex implements DruidPredicateIndex
  {
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();

            // todo: int iterator
            final Iterator<Integer> iterator = dictionary.iterator();
            int next;
            int index = 0;
            boolean nextSet = false;

            @Override
            public boolean hasNext()
            {
              if (!nextSet) {
                findNext();
              }
              return nextSet;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (!nextSet) {
                findNext();
                if (!nextSet) {
                  throw new NoSuchElementException();
                }
              }
              nextSet = false;

              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue == 0) {
                  nextSet = longPredicate.applyNull();
                } else {
                  nextSet = longPredicate.applyLong(globalLongDictionary.get(nextValue - adjustLongId));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }

  private class NestedDoubleLiteralValueSetIndex implements StringValueSetIndex
  {

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final Double doubleValue = Doubles.tryParse(value);
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          if (doubleValue == null) {
            return (double) getBitmap(dictionary.indexOf(0)).size() / totalRows;
          }
          return (double) getBitmap(dictionary.indexOf(globalDoubleDictionary.indexOf(doubleValue) + adjustDoubleId)).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          if (doubleValue == null) {
            return bitmapResultFactory.wrapDimensionValue(getBitmap(dictionary.indexOf(0)));
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(dictionary.indexOf(globalDoubleDictionary.indexOf(doubleValue) + adjustDoubleId)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          DoubleSet doubles = new DoubleArraySet(values.size());
          for (String value : values) {
            Double theValue = Doubles.tryParse(value);
            if (theValue != null) {
              doubles.add(theValue.doubleValue());
            }
          }
          return () -> new Iterator<ImmutableBitmap>()
          {
            final DoubleIterator iterator = doubles.iterator();
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
                double nextValue = iterator.nextDouble();
                next = dictionary.indexOf(globalDoubleDictionary.indexOf(nextValue) + adjustDoubleId);
              }
            }
          };
        }
      };
    }
  }

  private class NestedDoubleLiteralPredicateIndex implements DruidPredicateIndex
  {
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

            // todo: int iterator
            final Iterator<Integer> iterator = dictionary.iterator();
            int next;
            int index = 0;
            boolean nextSet = false;

            @Override
            public boolean hasNext()
            {
              if (!nextSet) {
                findNext();
              }
              return nextSet;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (!nextSet) {
                findNext();
                if (!nextSet) {
                  throw new NoSuchElementException();
                }
              }
              nextSet = false;
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue == 0) {
                  nextSet = doublePredicate.applyNull();
                } else {
                  nextSet = doublePredicate.applyDouble(globalDoubleDictionary.get(nextValue - adjustDoubleId));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }

  private abstract class NestedAnyLiteralIndex
  {
    int getIndex(@Nullable String value)
    {

      if (value == null) {
        return dictionary.indexOf(0);
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
  }

  /**
   * {@link StringValueSetIndex} but for variant typed nested literal columns
   */
  private class NestedAnyLiteralValueSetIndex extends NestedAnyLiteralIndex implements StringValueSetIndex
  {

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return (double) getBitmap(getIndex(value)).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.wrapDimensionValue(getBitmap(getIndex(value)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
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
      };
    }
  }

  /**
   * {@link DruidPredicateIndex} but for variant typed nested literal columns
   */
  private class NestedAnyLiteralPredicateIndex extends NestedAnyLiteralIndex implements DruidPredicateIndex
  {

    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Predicate<String> stringPredicate = matcherFactory.makeStringPredicate();
            final DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

            // todo: int iterator
            final Iterator<Integer> iterator = dictionary.iterator();
            int next;
            int index;
            boolean nextSet = false;

            @Override
            public boolean hasNext()
            {
              if (!nextSet) {
                findNext();
              }
              return nextSet;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (!nextSet) {
                findNext();
                if (!nextSet) {
                  throw new NoSuchElementException();
                }
              }
              nextSet = false;
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue >= adjustDoubleId) {
                  nextSet = doublePredicate.applyDouble(globalDoubleDictionary.get(nextValue - adjustDoubleId));
                } else if (nextValue >= adjustLongId) {
                  nextSet = longPredicate.applyLong(globalLongDictionary.get(nextValue - adjustLongId));
                } else {
                  nextSet = stringPredicate.apply(globalDictionary.get(nextValue));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }
}
