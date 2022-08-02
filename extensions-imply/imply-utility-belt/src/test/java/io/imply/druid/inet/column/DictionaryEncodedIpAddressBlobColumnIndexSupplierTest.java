/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DictionaryEncodedIpAddressBlobColumnIndexSupplierTest
{
  @Test
  public void testIpAddressWithIndex() throws IOException
  {
    DictionaryEncodedIpAddressBlobColumnIndexSupplier indexSupplier = IpAddressTestUtils.makeIpAddressIndexSupplier();

    DictionaryEncodedIpAddressBlobValueIndex valueIndex = indexSupplier.as(DictionaryEncodedIpAddressBlobValueIndex.class);
    Assert.assertNotNull(valueIndex);

    ImmutableBitmap bitmap = valueIndex.getBitmapForValue(ByteBuffer.wrap(IpAddressBlob.ofString("1.2.3.4").getBytes()));
    Assert.assertNotNull(bitmap);
    IpAddressTestUtils.checkBitmap(bitmap, 1, 3, 7, 8);

    // non-existent
    bitmap = valueIndex.getBitmapForValue(ByteBuffer.wrap(IpAddressBlob.ofString("9.2.3.4").getBytes()));
    Assert.assertNotNull(bitmap);
    IpAddressTestUtils.checkBitmap(bitmap);
  }

  @Test
  public void testIpPrefixWithIndex() throws IOException
  {
    DictionaryEncodedIpAddressBlobColumnIndexSupplier indexSupplier = IpAddressTestUtils.makeIpPrefixIndexSupplier();

    DictionaryEncodedIpAddressBlobValueIndex valueIndex = indexSupplier.as(DictionaryEncodedIpAddressBlobValueIndex.class);
    Assert.assertNotNull(valueIndex);

    ImmutableBitmap bitmap = valueIndex.getBitmapForValue(ByteBuffer.wrap(IpPrefixBlob.ofString("1.2.3.4/16").getBytes()));
    Assert.assertNotNull(bitmap);
    IpAddressTestUtils.checkBitmap(bitmap, 1, 3, 7, 8);

    // non-existent
    bitmap = valueIndex.getBitmapForValue(ByteBuffer.wrap(IpPrefixBlob.ofString("1.2.3.4/32").getBytes()));
    Assert.assertNotNull(bitmap);
    IpAddressTestUtils.checkBitmap(bitmap);
  }
}
