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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.GenericIndexed;

import java.nio.ByteBuffer;

public class IpAddressDictionaryEncodedColumnSupplier implements Supplier<IpAddressDictionaryEncodedColumn>
{
  private final GenericIndexed<ByteBuffer> dictionary;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final Supplier<ColumnarInts> column;

  public IpAddressDictionaryEncodedColumnSupplier(
      Supplier<ColumnarInts> column,
      GenericIndexed<ByteBuffer> dictionary,
      GenericIndexed<ImmutableBitmap> bitmaps
  )
  {
    this.column = column;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public IpAddressDictionaryEncodedColumn get()
  {
    return new IpAddressDictionaryEncodedColumn(column.get(), dictionary, bitmaps);
  }
}
