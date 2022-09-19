/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.google.common.base.Suppliers;
import io.imply.druid.segment.serde.simpletimeseries.IntegerDeltaEncoderDecoder;
import io.imply.druid.segment.serde.simpletimeseries.IntegerDeltaTimestampsEncoderDecoder;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerde;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesTestUtil;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleTimeSeriesBufferTest
{
  private static final DateTime START_DATE_TIME = SimpleTimeSeriesTestUtil.START_DATE_TIME;
  private static final SimpleTimeSeries TIME_SERIES = new SimpleTimeSeries(
      new ImplyLongArrayList(new long[]{
          START_DATE_TIME.getMillis(),
          START_DATE_TIME.plusMillis(1).getMillis(),
          START_DATE_TIME.plusMillis(10).getMillis(),
          START_DATE_TIME.plusMillis(100).getMillis()
      }),
      new ImplyDoubleArrayList(
          new double[]{
              11.0,
              37.0,
              23.0,
              53.0
          }),
      SimpleTimeSeriesTestUtil.ALL_TIME_INTERVAL,
      Integer.MAX_VALUE
  );
  private static final SimpleTimeSeriesSerde TIME_SERIES_SERDE = new SimpleTimeSeriesSerde(
      new IntegerDeltaTimestampsEncoderDecoder(new IntegerDeltaEncoderDecoder(START_DATE_TIME.getMillis()))
  );
  private static final ByteBuffer ENCODED_TIME_SERIES =
      ByteBuffer.wrap(TIME_SERIES_SERDE.serialize(TIME_SERIES)).order(ByteOrder.nativeOrder());

  @Test
  public void testIsNull()
  {
    SimpleTimeSeriesBuffer buffer = new SimpleTimeSeriesBuffer(
        TIME_SERIES_SERDE,
        Suppliers.ofInstance(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER)
    );

    Assert.assertTrue(buffer.isNull());
  }

  @Test
  public void testNonNull()
  {
    SimpleTimeSeriesBuffer buffer =
        new SimpleTimeSeriesBuffer(TIME_SERIES_SERDE, Suppliers.ofInstance(ENCODED_TIME_SERIES));

    Assert.assertFalse(buffer.isNull());
  }
}
