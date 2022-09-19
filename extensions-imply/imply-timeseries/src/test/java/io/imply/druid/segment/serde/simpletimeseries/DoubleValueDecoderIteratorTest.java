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
import java.util.NoSuchElementException;

public class DoubleValueDecoderIteratorTest
{
  private ImplyDoubleArrayList rawList;
  private ImplyDoubleArrayList rleList;
  private double[] rawDoubleArray;
  private double[] rleDoubleArray;
  private ByteBuffer rawListByteBuffer;
  private ByteBuffer rleByteBuffer;

  @Before
  public void setup()
  {
    rleDoubleArray = new double[]{
        37.0,
        37.0,
        1.0,
        100.0,
        100.0,
        100.0,
        100.0,
        2.0,
        3.0,
        3.0,
        4.0,
        4.0,
        4.0,
        6.0
    };

    rleList = new ImplyDoubleArrayList(rleDoubleArray);
    rawDoubleArray = new double[]{
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
        6.0,
        100.0,
        50.0
        };
    rawList = new ImplyDoubleArrayList(rawDoubleArray);
    rawListByteBuffer = createEncodedByteBuffer(rawList);
    rleList = new ImplyDoubleArrayList(rleDoubleArray);
    rleByteBuffer = createEncodedByteBuffer(rleList);
  }

  @Test
  public void testRawList()
  {
    DoubleIterator datapointsIterator = new DoubleValueRawDecoderIterator(rawListByteBuffer);

    compareArrayAndIterator(rawDoubleArray, datapointsIterator);
  }

  @Test
  public void testRle()
  {
    DoubleIterator datapointsIterator = new DoubleValueRleDecoderIterator(rleByteBuffer);

    compareArrayAndIterator(rleDoubleArray, datapointsIterator);
  }


  @Test
  public void testRawListBaseDecodeer()
  {
    DoubleIterator datapointsIterator =
        DoubleValueDecoderIterator.createRawListDecoderIterator(rawListByteBuffer);

    compareArrayAndIterator(rawDoubleArray, datapointsIterator);
  }

  @Test
  public void testRleListBaseDecodeer()
  {
    DoubleIterator datapointsIterator =
        DoubleValueDecoderIterator.createRleDecoderIterator(rleByteBuffer);

    compareArrayAndIterator(rleDoubleArray, datapointsIterator);
  }

  @Test
  public void testEmptyByteBufferRle()
  {
    DoubleIterator datapointsIterator =
           DoubleValueDecoderIterator.createRleDecoderIterator(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER);
    Assert.assertFalse(datapointsIterator.hasNext());

  }

  @Test
  public void testEmptyByteBufferRaw()
  {
    DoubleIterator datapointsIterator =
           DoubleValueDecoderIterator.createRawListDecoderIterator(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER);
    Assert.assertFalse(datapointsIterator.hasNext());

  }

  private ByteBuffer createEncodedByteBuffer(ImplyDoubleArrayList arrayList)
  {
    StorableList enocded = DoubleValuesEncoderDecoder.encode(arrayList);
    ByteBuffer byteBuffer = ByteBuffer.allocate(enocded.getSerializedSize());

    enocded.store(byteBuffer);
    byteBuffer.flip();

    return byteBuffer;
  }

  private void compareArrayAndIterator(double[] expectedArr, DoubleIterator timestampIterator)
  {

    for (double expected : expectedArr) {
      Assert.assertTrue(timestampIterator.hasNext());
      Assert.assertEquals(expected, timestampIterator.next(), 0);
    }

    Assert.assertFalse("more elements in iterator than expected", timestampIterator.hasNext());
    try {
      timestampIterator.next();
      Assert.fail("expected exception");
    }
    catch (NoSuchElementException e) {
      //expected
    }
  }
}
