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
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;


public class SimpleTimeSeriesObjectStrategyTest
{
  private final Random random = new Random(100);
  private final int numEntries = 1000;
  private SimpleTimeSeries timeSeries;
  private final SimpleTimeSeriesObjectStrategy objectStrategy = new SimpleTimeSeriesObjectStrategy();

  @Before
  public void setup()
  {
    DateTime startDateTime = DateTimes.of("2020-01-01");
    timeSeries = new SimpleTimeSeries(SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, Integer.MAX_VALUE);

    for (int i = numEntries; i >= 0; i--) {
      timeSeries.addDataPoint(startDateTime.plusHours(i).getMillis(), i);
    }
  }

  @Test
  public void testSort()
  {
    byte[] bytes = objectStrategy.toBytes(timeSeries);
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
    SimpleTimeSeries afterSortTimeSeries = objectStrategy.fromByteBuffer(byteBuffer, bytes.length);
    ImplyLongArrayList timestamps = afterSortTimeSeries.getTimestamps();
    ImplyDoubleArrayList dataPoints = afterSortTimeSeries.getDataPoints();

    long lastTimestamp = timestamps.getLong(0);
    double lastDataPoint = dataPoints.getDouble(0);

    for (int i = 1; i < timestamps.size(); i++) {
      Assert.assertTrue(timestamps.getLong(i) > lastTimestamp);
      Assert.assertTrue(dataPoints.getDouble(i) > lastDataPoint);
      lastTimestamp = timestamps.getLong(i);
      lastDataPoint = dataPoints.getDouble(i);
    }
  }

  @Test
  public void testNull()
  {
    byte[] bytes = objectStrategy.toBytes(null);
    Assert.assertEquals(bytes.length, 0);
  }

  @Test
  public void testVariedSize()
  {
    int rowCount = 500;
    int maxDataPointCount = 16 * 1024;
    int totalCount = 0;

    for (int i = 0; i < rowCount; i++) {
      int dataPointCount = random.nextInt(maxDataPointCount);
      SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(dataPointCount, totalCount);
      totalCount += dataPointCount;
      totalCount = Math.max(totalCount, 0);

      byte[] bytes = objectStrategy.toBytes(simpleTimeSeries);
      SimpleTimeSeries deserialized = objectStrategy.fromByteBuffer(
          ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()), bytes.length
      );
      Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), deserialized.asSimpleTimeSeriesData());
    }
  }
}
