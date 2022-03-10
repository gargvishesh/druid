/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

public abstract class IpAddressBitmapIndex implements BitmapIndex
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<ByteBuffer> dictionary;

  public IpAddressBitmapIndex(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      GenericIndexed<ByteBuffer> dictionary
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  @Override
  public boolean hasNulls()
  {
    return dictionary.indexOf(null) >= 0;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public int getIndex(@Nullable String value)
  {
    // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
    if (value == null) {
      return dictionary.indexOf(null);
    }
    return dictionary.indexOf(ByteBuffer.wrap(IpAddressBlob.ofString(value).getBytes()));
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
    final int idx = dictionary.indexOf(ByteBuffer.wrap(IpAddressBlob.ofString(value).getBytes()));
    return getBitmap(idx);
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
        while (currIndex < end) {
          IpAddressBlob blob = IpAddressBlob.ofByteBuffer(dictionary.get(currIndex));
          String blobString = blob == null ? blob.asCompressedString() : null;
          if (indexMatcher.test(blobString)) {
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
          next = dictionary.indexOf(ByteBuffer.wrap(IpAddressBlob.ofString(nextValue).getBytes()));
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
      IpAddressBlob blob = IpAddressBlob.ofString(startValue);
      final int found = dictionary.indexOf(blob == null ? null : ByteBuffer.wrap(blob.getBytes()));
      if (found >= 0) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (endValue == null) {
      endIndex = dictionary.size();
    } else {
      IpAddressBlob blob = IpAddressBlob.ofString(endValue);
      final int found = dictionary.indexOf(blob == null ? null : ByteBuffer.wrap(blob.getBytes()));
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
