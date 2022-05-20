/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.google.common.base.Predicate;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class DictionaryEncodedIpAddressBlobValueIndex
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<ByteBuffer> dictionary;

  public DictionaryEncodedIpAddressBlobValueIndex(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      GenericIndexed<ByteBuffer> dictionary
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
  }


  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }


  public int getIndex(@Nullable ByteBuffer blob)
  {
    // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
    if (blob == null) {
      return dictionary.indexOf(null);
    }
    return dictionary.indexOf(blob);
  }


  public ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }



  public ImmutableBitmap getBitmapForValue(@Nullable ByteBuffer blob)
  {
    final int idx = dictionary.indexOf(blob);
    return getBitmap(idx);
  }


  public Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable ByteBuffer startValue,
      boolean startStrict,
      @Nullable ByteBuffer endValue,
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


  public Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable ByteBuffer startValue,
      boolean startStrict,
      @Nullable ByteBuffer endValue,
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
        while (currIndex < end) {
          IpAddressBlob blob = IpAddressBlob.ofByteBuffer(dictionary.get(currIndex));
          String blobString = blob == null ? null : blob.asCompressedString();
          if (indexMatcher.apply(blobString)) {
            break;
          }
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


  public Iterable<ImmutableBitmap> getBitmapsForValues(Set<ByteBuffer> values)
  {
    return () -> new Iterator<ImmutableBitmap>()
    {
      final Iterator<ByteBuffer> iterator = values.iterator();
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
          ByteBuffer nextValue = iterator.next();
          next = dictionary.indexOf(nextValue);
        }
      }
    };
  }

  private IntIntPair getRange(@Nullable ByteBuffer startValue, boolean startStrict, @Nullable ByteBuffer endValue, boolean endStrict)
  {
    int startIndex, endIndex;
    if (startValue == null) {
      startIndex = 0;
    } else {
      final int found = dictionary.indexOf(startValue);
      if (found >= 0) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (endValue == null) {
      endIndex = dictionary.size();
    } else {
      final int found = dictionary.indexOf(endValue);
      if (found >= 0) {
        endIndex = endStrict ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }

    endIndex = Math.max(startIndex, endIndex);
    return new IntIntImmutablePair(startIndex, endIndex);
  }

  public Iterable<ImmutableBitmap> getBitmapsForPredicateFactory(DruidPredicateFactory predicateFactory)
  {
    return () -> new Iterator<ImmutableBitmap>()
    {
      // this should probably actually use the object predicate, but most filters don't currently use the object predicate...
      final Predicate<String> stringPredicate = predicateFactory.makeStringPredicate();
      final Iterator<ByteBuffer> iterator = dictionary.iterator();
      @Nullable
      ByteBuffer next = null;
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
        final int idx = dictionary.indexOf(next);
        if (idx < 0) {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }

        final ImmutableBitmap bitmap = bitmaps.get(idx);
        return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
      }

      private void findNext()
      {
        while (!nextSet && iterator.hasNext()) {
          ByteBuffer nextValue = iterator.next();
          if (nextValue == null) {
            nextSet = stringPredicate.apply(null);
          } else {
            final IpAddressBlob blob = IpAddressBlob.ofByteBuffer(nextValue);
            nextSet = stringPredicate.apply(blob.asCompressedString());
          }
          if (nextSet) {
            next = nextValue;
          }
        }
      }
    };
  }
}
