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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerdeTest;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerdeTestBase;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesTestUtil;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesTestingSerde;
import io.imply.druid.segment.serde.simpletimeseries.TestCaseResult;
import io.imply.druid.segment.serde.simpletimeseries.TestCasesConfig;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SimpleTimeSeriesContainerTest extends SimpleTimeSeriesSerdeTestBase
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

  public SimpleTimeSeriesContainerTest()
  {
    super(
        new SimpleTimeSeriesContainerTestingSerde(SimpleTimeSeriesTestUtil.ALL_TIME_INTERVAL, 1 << 16),
        new TestCasesConfig<>(SimpleTimeSeriesSerdeTest.class, SimpleTimeSeriesSerdeTestBase.class)
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testNull, TestCaseResult.of(new byte[]{1}))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testEmptyList, TestCaseResult.of(new byte[]{1}))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleItemList, TestCaseResult.of(58)) // special case
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testTwoItemList, TestCaseResult.of(78))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testLargerList, TestCaseResult.of(870))
            .setTestCaseValue(SimpleTimeSeriesSerdeTest::testSingleValueRun, TestCaseResult.of(82))
    );
  }

  @Test
  public void testWithEdges()
  {
    Interval interval = Intervals.of("2020-01-01T02:30/2020-01-01T05:30");
    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(
        0.0,
        100.0,
        DateTimes.of("2020-01-01T01"),
        DateTimes.of("2020-01-02T02"),
        DateTimes.of("2020-01-03T03"),
        DateTimes.of("2020-01-01T04"),
        DateTimes.of("2020-01-01T05"),
        DateTimes.of("2020-01-01T06")
    ).withWindow(interval);
    byte[] bytes = SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries).getSerializedBytes();
    Assert.assertEquals(78, bytes.length);
    SimpleTimeSeries deserialized = SimpleTimeSeriesContainer.createFromByteBuffer(
        ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()),
        interval,
        Integer.MAX_VALUE
    ).getSimpleTimeSeries();
    Assert.assertEquals(simpleTimeSeries, deserialized);
  }

  @Test
  public void testIsNullInstance()
  {
    SimpleTimeSeriesContainer container = SimpleTimeSeriesContainer.createFromInstance(null);

    Assert.assertTrue(container.isNull());
  }

  @Test
  public void testNonNullInstance()
  {
    SimpleTimeSeriesContainer holder = SimpleTimeSeriesContainer.createFromInstance(TIME_SERIES);

    Assert.assertFalse(holder.isNull());
  }

  @Test
  public void testIsNullBuffer()
  {
    SimpleTimeSeriesBuffer timeSeriesBuffer =
        new SimpleTimeSeriesBuffer(TIME_SERIES_SERDE, Suppliers.ofInstance(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER));
    SimpleTimeSeriesContainer container = SimpleTimeSeriesContainer.createFromBuffer(timeSeriesBuffer);

    Assert.assertTrue(container.isNull());
  }

  @Test
  public void testNonNullBuffer()
  {
    SimpleTimeSeriesBuffer timeSeriesBuffer =
        new SimpleTimeSeriesBuffer(TIME_SERIES_SERDE, Suppliers.ofInstance(ENCODED_TIME_SERIES));

    SimpleTimeSeriesContainer holder = SimpleTimeSeriesContainer.createFromBuffer(timeSeriesBuffer);

    Assert.assertFalse(holder.isNull());
  }

  public static class SimpleTimeSeriesContainerTestingSerde implements SimpleTimeSeriesTestingSerde
  {
    private final Interval window;
    private final int maxEntries;

    public SimpleTimeSeriesContainerTestingSerde(Interval window, int maxEntries)
    {
      this.window = window;
      this.maxEntries = maxEntries;
    }

    @Override
    public byte[] serialize(SimpleTimeSeries simpleTimeSeries)
    {
      return SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries).getSerializedBytes();
    }

    @Override
    public SimpleTimeSeries deserialize(ByteBuffer byteBuffer)
    {
      return SimpleTimeSeriesContainer.createFromByteBuffer(byteBuffer, window, maxEntries).getSimpleTimeSeries();
    }
  }

}
