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

public class LongDeltaTimestampsEncoderDecoderTest
{
  private static final DateTime START_DATE_TIME = SimpleTimeSeriesTestUtil.START_DATE_TIME;
  private static final DateTime DATE_TIME_PLUS_INT_MAX = START_DATE_TIME.plusMillis(Integer.MAX_VALUE);

  private static final LongDeltaTimestampsEncoderDecoder ENCODER_DECODER =
      new LongDeltaTimestampsEncoderDecoder(new LongDeltaEncoderDecoder(START_DATE_TIME.getMillis()));
  private static final ImplyLongArrayList RAW_LIST = new ImplyLongArrayList(
      new long[]{
          START_DATE_TIME.plusMillis(0).getMillis(),
          START_DATE_TIME.plusMillis(5).getMillis(),
          START_DATE_TIME.plusMillis(100).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(0).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(100).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(10000).getMillis(),
      }
  );

  private static final ImplyLongArrayList RLE_LIST = new ImplyLongArrayList(
      new long[]{
          START_DATE_TIME.plusMillis(0).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(0).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(1).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(2).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(3).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(4).getMillis(),
          DATE_TIME_PLUS_INT_MAX.plusMillis(5).getMillis(),
      }
  );

  @Test
  public void testRawSimple()
  {
    ByteBuffer byteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(RAW_LIST, ENCODER_DECODER, false);

    Assert.assertEquals(52, byteBuffer.remaining());

    ImplyLongArrayList decoded = ENCODER_DECODER.decode(byteBuffer, false);

    Assert.assertEquals(RAW_LIST, decoded);
  }

  @Test
  public void testRleSimple()
  {
    ByteBuffer byteBuffer = SimpleTimeSeriesTestUtil.createEncodedByteBuffer(RLE_LIST, ENCODER_DECODER, true);

    Assert.assertEquals(40, byteBuffer.remaining());

    ImplyLongArrayList decoded = ENCODER_DECODER.decode(byteBuffer, true);

    Assert.assertEquals(RLE_LIST, decoded);

  }

  @Test
  public void testEmptyList()
  {

  }

}
