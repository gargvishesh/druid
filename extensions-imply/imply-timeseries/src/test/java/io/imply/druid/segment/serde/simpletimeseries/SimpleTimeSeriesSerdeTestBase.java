/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.SimpleTimeSeries;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class SimpleTimeSeriesSerdeTestBase implements SimpleTimeSeriesSerdeTest
{
  private static final SimpleTimeSeries EMPTY_SIMPLE_TIME_SERIES = SimpleTimeSeriesTestUtil.buildTimeSeries(0, 0);

  protected final StagedSerde<SimpleTimeSeries> serializer;
  protected final TestCasesConfig<SimpleTimeSeriesSerdeTest> testCasesConfig;

  public SimpleTimeSeriesSerdeTestBase(
      StagedSerde<SimpleTimeSeries> serializer,
      TestCasesConfig<SimpleTimeSeriesSerdeTest> testCasesConfig
  )
  {
    this.serializer = serializer;
    this.testCasesConfig = testCasesConfig;
  }

  @Test
  @Override
  public void testNull()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());
    Assert.assertArrayEquals(testCasesConfig.currentTestValue().bytes, serializer.serialize(null));
  }

  @Test
  @Override
  public void testEmptyList()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());
    Assert.assertArrayEquals(
        testCasesConfig.currentTestValue().bytes,
        serializer.serialize(EMPTY_SIMPLE_TIME_SERIES)
    );
  }

  @Test
  @Override
  public void testSingleItemList()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());

    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(1, 0);
    byte[] bytes = serializer.serialize(simpleTimeSeries);
    Assert.assertEquals(testCasesConfig.currentTestValue().size, bytes.length);
    SimpleTimeSeries deserialized =
        serializer.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), deserialized.asSimpleTimeSeriesData());
  }

  @Test
  @Override
  public void testTwoItemList()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());

    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(2, 0);
    byte[] bytes = serializer.serialize(simpleTimeSeries);
    Assert.assertEquals(testCasesConfig.currentTestValue().size, bytes.length);
    SimpleTimeSeries deserialized =
        serializer.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), deserialized.asSimpleTimeSeriesData());
  }

  @Test
  @Override
  public void testLargerList()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());

    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(100, 0);
    byte[] bytes = serializer.serialize(simpleTimeSeries);
    Assert.assertEquals(testCasesConfig.currentTestValue().size, bytes.length);
    SimpleTimeSeries deserialized =
        serializer.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), deserialized.asSimpleTimeSeriesData());
  }

  @Test
  @Override
  public void testSingleValueRun()
  {
    Assume.assumeTrue(testCasesConfig.isCurrentTestEnabled());

    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeriesWithValueRuns(10, 0, 10);
    byte[] bytes = serializer.serialize(simpleTimeSeries);
    // 4 byte header; timestamps delta + rle encode to 2,{(1,0), (9,1)} and values encode to 1,{10,0}
    Assert.assertEquals(testCasesConfig.currentTestValue().size, bytes.length);
    SimpleTimeSeries deserialized =
        serializer.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), deserialized.asSimpleTimeSeriesData());
  }

}
