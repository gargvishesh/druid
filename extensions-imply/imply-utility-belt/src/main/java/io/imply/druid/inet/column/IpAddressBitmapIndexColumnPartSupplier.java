/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class IpAddressBitmapIndexColumnPartSupplier implements Supplier<BitmapIndex>
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<ByteBuffer> dictionary;


  public IpAddressBitmapIndexColumnPartSupplier(
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
  public BitmapIndex get()
  {
    return new BitmapIndex()
    {
      @Override
      public int getCardinality()
      {
        return dictionary.size();
      }

      @Override
      public String getValue(int index)
      {
        ByteBuffer value = dictionary.get(index);
        if (value == null) {
          return null;
        }
        return IpAddressBlob.ofByteBuffer(value).asCompressedString();
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
    };
  }
}

