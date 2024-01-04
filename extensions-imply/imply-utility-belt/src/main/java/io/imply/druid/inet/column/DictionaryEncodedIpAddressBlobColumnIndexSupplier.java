/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class DictionaryEncodedIpAddressBlobColumnIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<ByteBuffer> dictionary;
  private final Function<ByteBuffer, Object> byteBufferConversionFunction;

  public DictionaryEncodedIpAddressBlobColumnIndexSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      GenericIndexed<ByteBuffer> dictionary,
      Function<ByteBuffer, Object> byteBufferConversionFunction
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
    this.byteBufferConversionFunction = byteBufferConversionFunction;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(DictionaryEncodedIpAddressBlobValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
      return (T) new DictionaryEncodedIpAddressBlobValueIndex(
          bitmapFactory,
          bitmaps,
          dictionary,
          byteBufferConversionFunction
      );
    }
    return null;
  }
}
