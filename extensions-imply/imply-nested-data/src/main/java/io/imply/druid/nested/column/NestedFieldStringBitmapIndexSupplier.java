/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

public class NestedFieldStringBitmapIndexSupplier implements Supplier<BitmapIndex>
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<Integer> dictionary;
  private final GenericIndexed<String> globalDictionary;


  public NestedFieldStringBitmapIndexSupplier(
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
  public BitmapIndex get()
  {
    return new NestedFieldStringBitmapIndex(bitmapFactory, bitmaps, dictionary, globalDictionary);
  }
}
