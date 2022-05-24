/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import junit.framework.TestCase;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class NestedFieldLiteralColumnIndexSupplierTest extends TestCase
{

  @Test
  public void testRangeValueSkipping()
  {
    FixedIndexed<Integer> localDictionary = EasyMock.createMock(FixedIndexed.class);
    GenericIndexed<String> stringDictionary = EasyMock.createMock(GenericIndexed.class);
    FixedIndexed<Long> longDictionary = EasyMock.createMock(FixedIndexed.class);
    FixedIndexed<Double> doubleDictionary = EasyMock.createMock(FixedIndexed.class);
    GenericIndexed<ImmutableBitmap> bitmaps = EasyMock.createMock(GenericIndexed.class);

    RoaringBitmapFactory bitmapFactory = new RoaringBitmapFactory();
    MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();
    bitmap.add(1);
    ImmutableBitmap immutableBitmap = bitmapFactory.makeImmutableBitmap(bitmap);
    MutableBitmap bitmap2 = bitmapFactory.makeEmptyMutableBitmap();
    bitmap2.add(2);
    ImmutableBitmap immutableBitmap2 = bitmapFactory.makeImmutableBitmap(bitmap2);

    EasyMock.expect(stringDictionary.size()).andReturn(10).times(3);
    EasyMock.expect(longDictionary.size()).andReturn(0).times(1);
    EasyMock.expect(stringDictionary.indexOf("fo")).andReturn(3).times(2);
    EasyMock.expect(stringDictionary.indexOf("fooo")).andReturn(5).times(2);
    EasyMock.expect(stringDictionary.get(3)).andReturn("fo").times(1);
    EasyMock.expect(stringDictionary.get(4)).andReturn("foo").times(1);
    EasyMock.expect(stringDictionary.get(5)).andReturn("fooo").times(1);
    EasyMock.expect(localDictionary.indexOf(3)).andReturn(0).times(2);
    EasyMock.expect(localDictionary.indexOf(4)).andReturn(0).times(2);
    EasyMock.expect(localDictionary.indexOf(5)).andReturn(1).times(2);
    EasyMock.expect(localDictionary.indexOf(6)).andReturn(-2).times(2);
    EasyMock.expect(bitmaps.get(0)).andReturn(immutableBitmap).times(1);
    EasyMock.expect(bitmaps.get(1)).andReturn(immutableBitmap2).times(2);

    EasyMock.replay(localDictionary, stringDictionary, longDictionary, doubleDictionary, bitmaps);

    NestedFieldLiteralColumnIndexSupplier indexSupplier = new NestedFieldLiteralColumnIndexSupplier(
        new NestedLiteralTypeInfo.TypeSet(new NestedLiteralTypeInfo.MutableTypeSet().add(ColumnType.STRING).getByteValue()),
        bitmapFactory,
        bitmaps,
        localDictionary,
        stringDictionary,
        longDictionary,
        doubleDictionary
    );

    LexicographicalRangeIndex rangeIndex = indexSupplier.as(LexicographicalRangeIndex.class);

    BitmapColumnIndex columnIndex = rangeIndex.forRange("fo", false, "fooo", false);
    DefaultBitmapResultFactory defaultBitmapResultFactory = new DefaultBitmapResultFactory(bitmapFactory);
    ImmutableBitmap result = columnIndex.computeBitmapResult(defaultBitmapResultFactory);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.get(1));
    Assert.assertTrue(result.get(2));

    // predicate skips first index
    columnIndex = rangeIndex.forRange("fo", false, "fooo", false, "fooo"::equals);
    result = columnIndex.computeBitmapResult(defaultBitmapResultFactory);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(2));
    EasyMock.verify(localDictionary, stringDictionary, longDictionary, doubleDictionary, bitmaps);
  }
}
