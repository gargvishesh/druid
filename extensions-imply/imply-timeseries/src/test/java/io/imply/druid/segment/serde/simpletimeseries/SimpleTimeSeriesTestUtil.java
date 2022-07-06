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
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class SimpleTimeSeriesTestUtil
{
  public static final DateTime START_DATE_TIME = DateTimes.of("2020-01-01");
  public static final Interval ALL_TIME_INTERVAL = Intervals.utc(Long.MIN_VALUE, Long.MAX_VALUE);
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]).order(ByteOrder.nativeOrder());

  public static SimpleTimeSeries buildTimeSeries(DateTime start, int numDataPoints, int offset)
  {
    return buildTimeSeries(start, numDataPoints, offset, Integer.MAX_VALUE);
  }

  public static SimpleTimeSeries buildTimeSeries(DateTime start, int numDataPoints, int offset, int modulo)
  {
    SimpleTimeSeries simpleTimeSeries =
        new SimpleTimeSeries(SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, Integer.MAX_VALUE);

    for (int i = offset; i < offset + numDataPoints; i++) {
      int value = i % modulo;

      simpleTimeSeries.addDataPoint(start.plusMillis(value).getMillis(), value);
    }

    return simpleTimeSeries;
  }

  public static SimpleTimeSeries buildTimeSeriesWithValueRuns(int numDataPoints, int offset, int runLength)
  {
    return buildTimeSeriesWithValueRuns(START_DATE_TIME, numDataPoints, offset, runLength);
  }

  public static SimpleTimeSeries buildTimeSeriesWithValueRuns(
      DateTime start,
      int numDataPoints,
      int offset,
      int runLength
  )
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW,
        Integer.MAX_VALUE
    );
    int value = 0;

    for (int i = offset; i < offset + numDataPoints; i++) {
      if (i > offset && i % runLength == 0) {
        value++;
      }

      simpleTimeSeries.addDataPoint(start.plusMillis(i).getMillis(), value);
    }

    return simpleTimeSeries;
  }

  public static SimpleTimeSeries buildTimeSeries(int numDataPoints, int offset, int modulo)
  {
    return buildTimeSeries(START_DATE_TIME, numDataPoints, offset, modulo);
  }

  public static SimpleTimeSeries buildTimeSeries(int numDataPoints, int offset)
  {
    return buildTimeSeries(numDataPoints, offset, Integer.MAX_VALUE);
  }

  public static List<SimpleTimeSeries> buildTimeSeriesList(int rowCount, int entriesPerSeries) throws Exception
  {
    return buildTimeSeriesList(START_DATE_TIME, rowCount, entriesPerSeries);
  }


  public static List<SimpleTimeSeries> buildTimeSeriesList(DateTime startDateTime, int rowCount, int entriesPerSeries)
      throws Exception
  {
    return buildTimeSeriesList(startDateTime, rowCount, entriesPerSeries, SimpleTimeSeriesTestUtil::doNothing);
  }


  public static List<SimpleTimeSeries> buildTimeSeriesList(
      int rowCount,
      int entriesPerSeries,
      ExConsumer<SimpleTimeSeries, Exception> rowConsumer
  ) throws Exception
  {
    return buildTimeSeriesList(START_DATE_TIME, rowCount, entriesPerSeries, rowConsumer);
  }

  public static List<SimpleTimeSeries> buildTimeSeriesList(
      DateTime startDateTime,
      int rowCount,
      int entriesPerSeries,
      ExConsumer<SimpleTimeSeries, Exception> rowConsumer
  ) throws Exception
  {
    List<SimpleTimeSeries> rowList = new ArrayList<>(rowCount);

    for (int i = 0; i < rowCount; i++) {
      SimpleTimeSeries simpleTimeSeries = buildTimeSeries(startDateTime, entriesPerSeries, entriesPerSeries * i);

      rowList.add(simpleTimeSeries);

      rowConsumer.accept(simpleTimeSeries);
    }

    return rowList;
  }

  public static ByteBuffer createEncodedByteBuffer(
      ImplyLongArrayList arrayList,
      TimestampsEncoderDecoder encoderDecoder,
      boolean isRle
  )
  {
    StorableList encoded = encoderDecoder.encode(arrayList);

    Assert.assertEquals(isRle, encoded.isRle());

    ByteBuffer byteBuffer = ByteBuffer.allocate(encoded.getSerializedSize());

    encoded.store(byteBuffer);
    byteBuffer.flip();

    return byteBuffer;

  }

  private static void doNothing(SimpleTimeSeries row)
  {
  }
}
