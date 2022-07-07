/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class TimestampDecoderIteratorTest
{
  private IntegerDeltaTimestampsEncoderDecoder integerEncoderDecoder;
  private LongDeltaTimestampsEncoderDecoder longEncoderDecoder;
  private ImplyLongArrayList rawList;
  private ImplyLongArrayList rleList;
  private DateTime startDateTime;
  private long[] rawLongArr;
  private long[] rleLongArr;
  private ByteBuffer integerRawListByteBuffer;
  private ByteBuffer integerRleListByteBuffer;
  private ByteBuffer longRawListByteBuffer;
  private ByteBuffer longRleListByteBuffer;

  @Before
  public void setup()
  {
    startDateTime = DateTimes.of("2020-01-01");
    integerEncoderDecoder =
        new IntegerDeltaTimestampsEncoderDecoder(new IntegerDeltaEncoderDecoder(startDateTime.getMillis()));
    longEncoderDecoder =
        new LongDeltaTimestampsEncoderDecoder(new LongDeltaEncoderDecoder(startDateTime.getMillis()));

    // test varied length of runs including single-length runs interspersed
    rleLongArr = new long[]{
        startDateTime.getMillis(),
        startDateTime.plusMillis(1).getMillis(),
        startDateTime.plusMillis(2).getMillis(),
        startDateTime.plusMillis(3).getMillis(),
        startDateTime.plusMillis(4).getMillis(),
        startDateTime.plusMillis(5).getMillis(),
        startDateTime.plusMillis(7).getMillis(),
        startDateTime.plusMillis(8).getMillis(),
        startDateTime.plusMillis(9).getMillis(),
        startDateTime.plusMillis(12).getMillis(),
        startDateTime.plusMillis(13).getMillis(),
        startDateTime.plusMillis(14).getMillis(),
        };
    rleList = new ImplyLongArrayList(rleLongArr);
    rawLongArr = new long[]{
        startDateTime.getMillis(),
        startDateTime.plusMillis(1).getMillis(),
        startDateTime.plusMillis(3).getMillis(),
        startDateTime.plusMillis(6).getMillis(),
        startDateTime.plusMillis(10).getMillis(),
        startDateTime.plusMillis(15).getMillis(),
        };
    rawList = new ImplyLongArrayList(rawLongArr);
    integerRawListByteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(rawList, integerEncoderDecoder, false);
    longRawListByteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(rawList, longEncoderDecoder, false);
    integerRleListByteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(rleList, integerEncoderDecoder, true);
    longRleListByteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(rleList, longEncoderDecoder, true);
  }

  @Test
  public void testRawListInteger()
  {
    TimestampRawDecoderIterator timestampIterator =
        new TimestampRawDecoderIterator(true, startDateTime.getMillis(), integerRawListByteBuffer);

    compareArrayAndIterator(rawLongArr, timestampIterator);
  }

  @Test
  public void testRleInteger()
  {
    TimestampsRleDecoderIterator rleDecoderIterator =
        new TimestampsRleDecoderIterator(true, startDateTime.getMillis(), integerRleListByteBuffer);

    compareArrayAndIterator(rleLongArr, rleDecoderIterator);
  }

  @Test
  public void testRawListLong()
  {
    TimestampRawDecoderIterator timestampIterator =
        new TimestampRawDecoderIterator(false, startDateTime.getMillis(), longRawListByteBuffer);

    compareArrayAndIterator(rawLongArr, timestampIterator);
  }

  @Test
  public void testRleLong()
  {
    TimestampsRleDecoderIterator rleDecoderIterator =
        new TimestampsRleDecoderIterator(false, startDateTime.getMillis(), longRleListByteBuffer);

    compareArrayAndIterator(rleLongArr, rleDecoderIterator);
  }

  @Test
  public void testRawListIntegerBaseDecodeer()
  {
    TimestampDecoderIterator timestampIterator = TimestampDecoderIterator.createRawListIntegerDecoderIterator(
        startDateTime.getMillis(),
        integerRawListByteBuffer
    );

    compareArrayAndIterator(rawLongArr, timestampIterator);
  }

  @Test
  public void testRleListIntegerBaseDecodeer()
  {
    TimestampDecoderIterator timestampIterator =
        TimestampDecoderIterator.createRleIntegerDecoderIterator(startDateTime.getMillis(), integerRleListByteBuffer);

    compareArrayAndIterator(rleLongArr, timestampIterator);
  }

  @Test
  public void testRawListLongBaseDecodeer()
  {
    TimestampDecoderIterator timestampIterator =
        TimestampDecoderIterator.createRawListLongDecoderIterator(startDateTime.getMillis(), longRawListByteBuffer);

    compareArrayAndIterator(rawLongArr, timestampIterator);
  }

  @Test
  public void testRleListLongBaseDecodeer()
  {
    TimestampDecoderIterator timestampIterator =
        TimestampDecoderIterator.createRleLongDecoderIterator(startDateTime.getMillis(), longRleListByteBuffer);

    compareArrayAndIterator(rleLongArr, timestampIterator);
  }

  @Test
  public void testRleListLongEmptyBuffer()
  {
    TimestampDecoderIterator timestampIterator = TimestampDecoderIterator.createRleLongDecoderIterator(
        startDateTime.getMillis(),
        SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER
    );

    Assert.assertFalse(timestampIterator.hasNext());
  }

  @Test
  public void testRawistLongEmptyBuffer()
  {
    TimestampDecoderIterator timestampIterator = TimestampDecoderIterator.createRawListLongDecoderIterator(
        startDateTime.getMillis(),
        SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER
    );

    Assert.assertFalse(timestampIterator.hasNext());
  }

  @Test
  public void testRleListIntegerEmptyBuffer()
  {
    TimestampDecoderIterator timestampIterator = TimestampDecoderIterator.createRleIntegerDecoderIterator(
        startDateTime.getMillis(),
        SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER
    );

    Assert.assertFalse(timestampIterator.hasNext());
  }

  @Test
  public void testRawistIntegerEmptyBuffer()
  {
    TimestampDecoderIterator timestampIterator = TimestampDecoderIterator.createRawListIntegerDecoderIterator(
        startDateTime.getMillis(),
        SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER
    );

    Assert.assertFalse(timestampIterator.hasNext());
  }

  private void compareArrayAndIterator(long[] expectedArr, LongIterator timestampIterator)
  {

    for (long expected : expectedArr) {
      Assert.assertTrue(timestampIterator.hasNext());
      Assert.assertEquals(expected, timestampIterator.next());
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
