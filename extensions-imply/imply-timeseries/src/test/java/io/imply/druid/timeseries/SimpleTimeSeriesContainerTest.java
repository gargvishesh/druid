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
import io.imply.druid.segment.serde.simpletimeseries.TestCaseResult;
import io.imply.druid.segment.serde.simpletimeseries.TestCasesConfig;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
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
      Intervals.ETERNITY,
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
        new SimpleTimeSeriesContainerTestingSerde(Intervals.ETERNITY, 1 << 16),
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
    ).copyWithWindow(interval);
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
  public void testEmtpySeriesWithEdges()
  {
    // time interval from hour 1 to hour 2
    Interval interval = Intervals.of("2020-01-01T01:00/2020-01-01T02:00");
    TimeSeries.EdgePoint[] lefts = new TimeSeries.EdgePoint[]{
        null,
        null,
        new TimeSeries.EdgePoint(DateTimes.of("2020-01-01T00:00").getMillis(), 0),
        new TimeSeries.EdgePoint(DateTimes.of("2020-01-01T00:00").getMillis(), 0)
    };
    TimeSeries.EdgePoint[] rights = new TimeSeries.EdgePoint[]{
        null,
        new TimeSeries.EdgePoint(DateTimes.of("2020-01-01T02:00").getMillis(), 0),
        null,
        new TimeSeries.EdgePoint(DateTimes.of("2020-01-01T02:00").getMillis(), 0)
    };
    int[] serializedBytes = new int[]{1, 33, 33, 33};
    for (int i = 0; i < lefts.length; i++) {
      SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
          new ImplyLongArrayList(),
          new ImplyDoubleArrayList(),
          interval,
          lefts[i],
          rights[i],
          100,
          1L
      );
      byte[] bytes = SimpleTimeSeriesContainer.createFromInstance(simpleTimeSeries).getSerializedBytes();
      Assert.assertEquals(serializedBytes[i], bytes.length);
      SimpleTimeSeries deserialized = SimpleTimeSeriesContainer.createFromByteBuffer(
          ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()),
          interval,
          100
      ).getSimpleTimeSeries();
      if (lefts[i] == null && rights[i] == null) {
        Assert.assertNull(deserialized); // fully empty series are deserialized to nulls
      } else {
        Assert.assertEquals(simpleTimeSeries, deserialized);
      }
    }
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

  public static class SimpleTimeSeriesContainerTestingSerde implements StagedSerde<SimpleTimeSeries>
  {
    private final Interval window;
    private final int maxEntries;

    public SimpleTimeSeriesContainerTestingSerde(Interval window, int maxEntries)
    {
      this.window = window;
      this.maxEntries = maxEntries;
    }

    @Override
    public StorableBuffer serializeDelayed(@Nullable SimpleTimeSeries value)
    {
      byte[] serializedBytes = SimpleTimeSeriesContainer.createFromInstance(value).getSerializedBytes();

      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          byteBuffer.put(serializedBytes);
        }

        @Override
        public int getSerializedSize()
        {
          return serializedBytes.length;
        }
      };
    }

    @Override
    public byte[] serialize(SimpleTimeSeries value)
    {
      byte[] serializedBytes = SimpleTimeSeriesContainer.createFromInstance(value).getSerializedBytes();

      return serializedBytes;
    }

    @Override
    public SimpleTimeSeries deserialize(ByteBuffer byteBuffer)
    {
      return SimpleTimeSeriesContainer.createFromByteBuffer(byteBuffer, window, maxEntries).getSimpleTimeSeries();
    }
  }
}
