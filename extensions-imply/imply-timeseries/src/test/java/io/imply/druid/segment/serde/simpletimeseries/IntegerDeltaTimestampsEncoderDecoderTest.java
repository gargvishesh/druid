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
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class IntegerDeltaTimestampsEncoderDecoderTest
{
  private static final DateTime START_DATE_TIME = SimpleTimeSeriesTestUtil.START_DATE_TIME;

  private static final IntegerDeltaTimestampsEncoderDecoder ENCODER_DECODER =
      new IntegerDeltaTimestampsEncoderDecoder(new IntegerDeltaEncoderDecoder(START_DATE_TIME.getMillis()));
  private static final ImplyLongArrayList RAW_LIST = new ImplyLongArrayList(
      new long[]{
          START_DATE_TIME.plusMillis(0).getMillis(),
          START_DATE_TIME.plusMillis(5).getMillis(),
          START_DATE_TIME.plusMillis(10).getMillis(),
          START_DATE_TIME.plusMillis(20).getMillis(),
          START_DATE_TIME.plusMillis(35).getMillis(),
          START_DATE_TIME.plusMillis(75).getMillis(),
          START_DATE_TIME.plusMillis(100).getMillis(),
          START_DATE_TIME.plusMillis(1000).getMillis(),
          START_DATE_TIME.plusMillis(10000).getMillis(),
          START_DATE_TIME.plusMillis(100000).getMillis(),
          }
  );

  private static final ImplyLongArrayList RLE_LIST = new ImplyLongArrayList(
      new long[]{
          START_DATE_TIME.plusMillis(0).getMillis(),
          START_DATE_TIME.plusMillis(1).getMillis(),
          START_DATE_TIME.plusMillis(2).getMillis(),
          START_DATE_TIME.plusMillis(3).getMillis(),
          START_DATE_TIME.plusMillis(4).getMillis(),
          START_DATE_TIME.plusMillis(8).getMillis(),
          START_DATE_TIME.plusMillis(9).getMillis(),
          START_DATE_TIME.plusMillis(10).getMillis(),
          START_DATE_TIME.plusMillis(11).getMillis(),
          START_DATE_TIME.plusMillis(13).getMillis(),
          START_DATE_TIME.plusMillis(15).getMillis(),
          START_DATE_TIME.plusMillis(16).getMillis(),
          START_DATE_TIME.plusMillis(17).getMillis(),
          }
  );
  private static final ImplyLongArrayList EMPYT_LIST = new ImplyLongArrayList(0);

  @Test
  public void testRawSimple()
  {
    ByteBuffer byteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(RAW_LIST, ENCODER_DECODER, false);

    Assert.assertEquals(44, byteBuffer.remaining());

    ImplyLongArrayList decoded = ENCODER_DECODER.decode(byteBuffer, false);

    Assert.assertEquals(RAW_LIST, decoded);
  }

  @Test
  public void testRleSimple()
  {
    ByteBuffer byteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(RLE_LIST, ENCODER_DECODER, true);

    Assert.assertEquals(52, byteBuffer.remaining());

    ImplyLongArrayList decoded = ENCODER_DECODER.decode(byteBuffer, true);

    Assert.assertEquals(RLE_LIST, decoded);
  }

  @Test
  public void testEmpytList()
  {
    // empty list is always encoded as rle (doesn't affect space)
    ByteBuffer byteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(
        EMPYT_LIST,
        ENCODER_DECODER,
        true
    );

    Assert.assertEquals(4, byteBuffer.remaining());

    ImplyLongArrayList decoded = ENCODER_DECODER.decode(byteBuffer, true);

    Assert.assertEquals(EMPYT_LIST, decoded);
  }
}
