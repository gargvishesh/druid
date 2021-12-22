/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;

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
    // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
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
}
