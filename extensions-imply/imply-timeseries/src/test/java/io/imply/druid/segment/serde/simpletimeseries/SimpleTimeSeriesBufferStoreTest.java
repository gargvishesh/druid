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
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

public class SimpleTimeSeriesBufferStoreTest
{
  private final Random random = new Random(100);
  private final DateTime startDateTime = DateTimes.of("2020-01-01");
  private final SimpleTimeSeries[] simpleTimeSeriesArr = new SimpleTimeSeries[]{
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime, 100, 0),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime, 100, 100),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime, 100, 200),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime, 100, 300)
  };

  private final SimpleTimeSeries[] simpleTimeSeriesArrLongRange = new SimpleTimeSeries[]{
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime, 100, 0),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime.plusMillis(Integer.MAX_VALUE), 100, 100),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime.plusMillis(Integer.MAX_VALUE)
                                                            .plusMillis(Integer.MAX_VALUE), 100, 200),
      SimpleTimeSeriesTestUtil.buildTimeSeries(startDateTime.plusMillis(Integer.MAX_VALUE)
                                                            .plusMillis(Integer.MAX_VALUE)
                                                            .plusMillis(Integer.MAX_VALUE), 100, 300)
  };
  private final SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();

  private SimpleTimeSeriesBufferStore simpleTimeSeriesBufferStore;

  @Before
  public void setup() throws Exception
  {
    simpleTimeSeriesBufferStore = new SimpleTimeSeriesBufferStore(
        writeOutMedium.makeWriteOutBytes()
    );
  }

  @Test
  public void testIteratorSimple() throws Exception
  {
    for (SimpleTimeSeries simpleTimeSeries : simpleTimeSeriesArr) {
      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(simpleTimeSeriesArr[i].asSimpleTimeSeriesData(), iterator.next().asSimpleTimeSeriesData());
      i++;
    }
  }

  @Test
  public void testIteratorEmptyBuffer() throws Exception
  {
    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorNull() throws Exception
  {
    simpleTimeSeriesBufferStore.store(null);
    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();
    Assert.assertTrue(iterator.hasNext());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void testIteratorIdempotentHasNext() throws Exception
  {
    simpleTimeSeriesBufferStore.store(simpleTimeSeriesArr[0]);

    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();

    Assert.assertTrue(iterator.hasNext());
    Assert.assertTrue(iterator.hasNext());
  }

  @Test(expected = NoSuchElementException.class)
  public void testIteratorEmptyThrows() throws Exception
  {
    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();
    iterator.next();
  }

  @Test
  public void testIteratorEmptyHasNext() throws Exception
  {
    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testMinTimestampUsesInteger() throws Exception
  {
    for (SimpleTimeSeries simpleTimeSeries : simpleTimeSeriesArr) {
      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    SerializedColumnHeader columnHeader = simpleTimeSeriesBufferStore.createColumnHeader();
    Assert.assertEquals(startDateTime.getMillis(), columnHeader.getMinTimestamp());
    Assert.assertTrue(columnHeader.isUseIntegerDeltas());
  }

  @Test
  public void testMinTimestampUsesLong() throws Exception
  {
    for (SimpleTimeSeries simpleTimeSeries : simpleTimeSeriesArrLongRange) {
      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    SerializedColumnHeader columnHeader = simpleTimeSeriesBufferStore.createColumnHeader();
    Assert.assertEquals(startDateTime.getMillis(), columnHeader.getMinTimestamp());
    Assert.assertFalse(columnHeader.isUseIntegerDeltas());
  }

  @Test
  public void testMinTimestampUsesIntegerSerialization() throws Exception
  {
    for (SimpleTimeSeries simpleTimeSeries : simpleTimeSeriesArr) {
      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    SerializedColumnHeader columnHeader = simpleTimeSeriesBufferStore.createColumnHeader();

    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();
    try (ResourceHolder<ByteBuffer> resourceHolder = NativeClearedByteBufferProvider.DEFAULT.get()) {
      columnHeader.transferTo(channel);

      ByteBuffer byteBuffer = resourceHolder.get();
      channel.writeTo(byteBuffer);
      byteBuffer.flip();

      SerializedColumnHeader deserializedColumnhHeader = SerializedColumnHeader.fromBuffer(byteBuffer);
      Assert.assertEquals(startDateTime.getMillis(), deserializedColumnhHeader.getMinTimestamp());
      Assert.assertTrue(deserializedColumnhHeader.isUseIntegerDeltas());
    }
  }

  @Test
  public void testMinTimestampUsesLongSerialization() throws Exception

  {
    for (SimpleTimeSeries simpleTimeSeries : simpleTimeSeriesArrLongRange) {
      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    SerializedColumnHeader columnHeader = simpleTimeSeriesBufferStore.createColumnHeader();

    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();
    try (ResourceHolder<ByteBuffer> resourceHolder = NativeClearedByteBufferProvider.DEFAULT.get()) {
      columnHeader.transferTo(channel);

      ByteBuffer byteBuffer = resourceHolder.get();
      channel.writeTo(byteBuffer);
      byteBuffer.flip();

      SerializedColumnHeader deserializedColumnhHeader = SerializedColumnHeader.fromBuffer(byteBuffer);
      Assert.assertEquals(startDateTime.getMillis(), deserializedColumnhHeader.getMinTimestamp());
      Assert.assertFalse(deserializedColumnhHeader.isUseIntegerDeltas());
    }
  }

  @Test
  public void testVariedSize() throws Exception
  {
    // note: includes elements that are larger than 64k, which seems to be a read size limit

    int rowCount = 500;
    int maxDataPointCount = 16 * 1024;
    List<SimpleTimeSeries> input = new ArrayList<>(rowCount);
    int totalCount = 0;

    for (int i = 0; i < rowCount; i++) {
      int dataPointCount = random.nextInt(maxDataPointCount);
      SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(
          SimpleTimeSeriesTestUtil.START_DATE_TIME,
          dataPointCount,
          totalCount
      );
      input.add(simpleTimeSeries);
      totalCount += dataPointCount;
      totalCount = Math.max(totalCount, 0);

      simpleTimeSeriesBufferStore.store(simpleTimeSeries);
    }

    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();
    int i = 0;

    while (iterator.hasNext()) {
      Assert.assertEquals(input.get(i).asSimpleTimeSeriesData(), iterator.next().asSimpleTimeSeriesData());
      i++;
    }
  }

  @Test
  public void testLaregeBuffer() throws Exception
  {
    // note: test element larger than 64k

    int dataPointCount = 128 * 1024;
    SimpleTimeSeries simpleTimeSeries = SimpleTimeSeriesTestUtil.buildTimeSeries(
        SimpleTimeSeriesTestUtil.START_DATE_TIME,
        dataPointCount,
        0
    );
    simpleTimeSeriesBufferStore.store(simpleTimeSeries);

    IOIterator<SimpleTimeSeries> iterator = simpleTimeSeriesBufferStore.iterator();

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), iterator.next().asSimpleTimeSeriesData());
    Assert.assertFalse(iterator.hasNext());
  }
}
