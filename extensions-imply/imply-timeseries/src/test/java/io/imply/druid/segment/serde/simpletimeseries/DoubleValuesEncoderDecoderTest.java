/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DoubleValuesEncoderDecoderTest
{
  private static final ImplyDoubleArrayList SIMPLE_INPUT = new ImplyDoubleArrayList(new double[]{
      0.0,
      1.0,
      2.0,
      3.0
  });

  private static final ImplyDoubleArrayList RLE_INPUT = new ImplyDoubleArrayList(new double[]{
      0.0,
      0.0,
      0.0,
      100.0,
      22.0,
      22.0,
      22.0,
      22.0,
      1.0,
      1.0,
      2.0,
      3.0,
      3.0,
      3.0,
      3.0,
      3.0,
      3.0,
      3.0,
      1.0
  });
  private ByteBuffer zeroSizeList;

  @Before
  public void setup()
  {
    zeroSizeList = ByteBuffer.allocate(Integer.BYTES);
    zeroSizeList.putInt(0);
  }


  @Test
  public void testRawSimple()
  {
    StorableList encoded = DoubleValuesEncoderDecoder.encode(SIMPLE_INPUT);

    Assert.assertFalse(encoded.isRle());
    Assert.assertEquals(36, encoded.getSerializedSize());

    ByteBuffer byteBuffer = ByteBuffer.allocate(encoded.getSerializedSize());

    encoded.store(byteBuffer);
    byteBuffer.flip();

    ImplyDoubleArrayList decoded = DoubleValuesEncoderDecoder.decode(byteBuffer, encoded.isRle());

    Assert.assertEquals(SIMPLE_INPUT, decoded);
  }

  @Test
  public void testRleSimple()
  {
    StorableList encoded = DoubleValuesEncoderDecoder.encode(RLE_INPUT);

    Assert.assertTrue(encoded.isRle());
    Assert.assertEquals(88, encoded.getSerializedSize());

    ByteBuffer byteBuffer = ByteBuffer.allocate(encoded.getSerializedSize());

    encoded.store(byteBuffer);
    byteBuffer.flip();

    ImplyDoubleArrayList decoded = DoubleValuesEncoderDecoder.decode(byteBuffer, encoded.isRle());

    Assert.assertEquals(RLE_INPUT, decoded);
  }

  @Test
  public void testEmptyEncode()
  {
    StorableList encoded = DoubleValuesEncoderDecoder.encode(new ImplyDoubleArrayList());

    Assert.assertTrue(encoded.isRle());
    Assert.assertEquals(encoded.getSerializedSize(), Integer.BYTES);

    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);

    encoded.store(byteBuffer);

    Assert.assertEquals(Integer.BYTES, byteBuffer.position());
  }

  @Test
  public void testEmptyByteBufferDecode()
  {
    ImplyDoubleArrayList rleDecoded =
        DoubleValuesEncoderDecoder.decode(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER, true);

    Assert.assertEquals(0, rleDecoded.size());

    ImplyDoubleArrayList rawDecoded =
        DoubleValuesEncoderDecoder.decode(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER, false);

    Assert.assertEquals(0, rawDecoded.size());
  }

  @Test
  public void testEmpytListDecode()
  {
    ImplyDoubleArrayList rleDecoded = DoubleValuesEncoderDecoder.decode(zeroSizeList, true);

    Assert.assertEquals(0, rleDecoded.size());

    ImplyDoubleArrayList rawDecoded = DoubleValuesEncoderDecoder.decode(zeroSizeList, true);

    Assert.assertEquals(0, rawDecoded.size());
  }
}
