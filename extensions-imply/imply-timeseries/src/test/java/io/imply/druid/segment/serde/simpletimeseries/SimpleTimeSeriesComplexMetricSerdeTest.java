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
import io.imply.druid.timeseries.SimpleTimeSeriesData;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.writeout.HeapByteBufferWriteOutBytes;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleTimeSeriesComplexMetricSerdeTest
{
  private static final DateTime START_DATE_TIME = DateTimes.of("2020-01-01");

  private static SimpleTimeSeries SIMPLE_TIME_SERIES_1;
  private static SimpleTimeSeries SIMPLE_TIME_SERIES_2;

  private final Random random = new Random(100);

  private ResourceHolder<ByteBuffer> rowIndexUncompressedBlockHolder;
  private ResourceHolder<ByteBuffer> dataUncompressedBlockHolder;
  private SimpleTimeSeriesColumnSerializer columnSerializer;


  @BeforeClass
  public static void setUp()
  {
    SIMPLE_TIME_SERIES_1 = buildTimeSeries(START_DATE_TIME, 100, 0);
    SIMPLE_TIME_SERIES_2 = buildTimeSeries(START_DATE_TIME, 100, 100);
  }

  @Before
  public void setup() throws Exception
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    columnSerializer = new SimpleTimeSeriesColumnSerializer(new RowWriter.Builder(writeOutMedium));
    columnSerializer.open();
    rowIndexUncompressedBlockHolder = NativeClearedByteBufferProvider.DEFAULT.get();
    dataUncompressedBlockHolder = NativeClearedByteBufferProvider.DEFAULT.get();
  }

  @After
  public void tearDown()
  {
    rowIndexUncompressedBlockHolder.close();
    dataUncompressedBlockHolder.close();
  }

  @Test
  public void testEnd2EndSimple() throws Exception
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));
    //update if size is intended to change
    Assert.assertEquals(1684, columnSerializer.getSerializedSize());

    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();

    SimpleTimeSeries row1 = simpleTimeSeriesView.getRow(0);
    SimpleTimeSeries row2 = simpleTimeSeriesView.getRow(1);
    Assert.assertEquals(SIMPLE_TIME_SERIES_1.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
    Assert.assertEquals(SIMPLE_TIME_SERIES_2.asSimpleTimeSeriesData(), row2.asSimpleTimeSeriesData());
  }

  @Test
  public void testEnd2EndSimpleWithNull() throws Exception
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(null));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));
    //update if size is intended to change
    Assert.assertEquals(1684, columnSerializer.getSerializedSize());

    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();

    SimpleTimeSeries row1 = simpleTimeSeriesView.getRow(0);
    SimpleTimeSeries row2 = simpleTimeSeriesView.getRow(1);
    SimpleTimeSeries row3 = simpleTimeSeriesView.getRow(2);
    Assert.assertEquals(SIMPLE_TIME_SERIES_1.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
    Assert.assertNull(row2);
    Assert.assertEquals(SIMPLE_TIME_SERIES_2.asSimpleTimeSeriesData(), row3.asSimpleTimeSeriesData());
  }

  @Test
  public void testEnd2EndMultiBlock() throws Exception
  {
    int numberOfRows = 5000;
    int entriesPerSeries = 100;
    List<SimpleTimeSeries> sourceRowsList = new ArrayList<>(numberOfRows);
    for (int i = 0; i < numberOfRows; i++) {
      SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, entriesPerSeries, entriesPerSeries * i);
      sourceRowsList.add(simpleTimeSeries);
      columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    }
    //update if size is intended to change (note: > 64k block size means we are testing cross-block reads
    long serializedSize = columnSerializer.getSerializedSize();
    Assert.assertTrue(serializedSize > 2 * CompressedPools.BUFFER_SIZE);

    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();

    for (int i = 0; i < numberOfRows; i++) {
      SimpleTimeSeries row = simpleTimeSeriesView.getRow(i);
      Assert.assertNotNull(row);
      Assert.assertEquals(sourceRowsList.get(i).asSimpleTimeSeriesData(), row.asSimpleTimeSeriesData());
    }
  }

  @Test
  public void testMultiBlockSpanRow() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 256 * 1024, 0);
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    long serializedSize = columnSerializer.getSerializedSize();
    Assert.assertTrue(serializedSize > 2 * CompressedPools.BUFFER_SIZE);

    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();
    SimpleTimeSeries row1 = simpleTimeSeriesView.getRow(0);
    Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
  }

  @Test
  public void testEmptyTimeSeries() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 0, 0);
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();
    SimpleTimeSeries row = simpleTimeSeriesView.getRow(0);
    SimpleTimeSeriesData simpleTimeSeriesData = row.asSimpleTimeSeriesData();
    Assert.assertEquals(0, simpleTimeSeriesData.getDataPoints().size());
    Assert.assertEquals(0, simpleTimeSeriesData.getTimestamps().size());
  }

  @Test
  public void testEmptyTimeSeriesInterleaved() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 0, 0);
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));
    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();
    SimpleTimeSeries row = simpleTimeSeriesView.getRow(1);
    SimpleTimeSeriesData simpleTimeSeriesData = row.asSimpleTimeSeriesData();
    Assert.assertEquals(0, simpleTimeSeriesData.getDataPoints().size());
    Assert.assertEquals(0, simpleTimeSeriesData.getTimestamps().size());
  }

  @Test
  public void testVariedRowSize() throws Exception
  {
    int rowCount = 500;
    int maxDataPointCount = 16 * 1024;
    List<SimpleTimeSeries> input = new ArrayList<>();
    int totalCount = 0;
    for (int i = 0; i < rowCount; i++) {
      int dataPointCount = random.nextInt(maxDataPointCount);
      SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, dataPointCount, totalCount);
      input.add(simpleTimeSeries);
      totalCount += dataPointCount;
      totalCount = Math.max(totalCount, 0);

      columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    }

    SimpleTimeSeriesView simpleTimeSeriesView = createSimpleTimeSeriesView();

    for (int i = 0; i < rowCount; i++) {
      SimpleTimeSeries row = simpleTimeSeriesView.getRow(i);
      Assert.assertEquals(input.get(i).asSimpleTimeSeriesData(), row.asSimpleTimeSeriesData());
    }
  }

  @Test
  public void testSingleNull() throws Exception
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(null));

    ByteBuffer byteBuffer = serializeToByteBuffer(columnSerializer);
    Assert.assertEquals(46, columnSerializer.getSerializedSize());

    SimpleTimeSeriesView simpleTimeSeriesView = SimpleTimeSeriesView.create(byteBuffer,
                                                                            rowIndexUncompressedBlockHolder.get(),
                                                                            dataUncompressedBlockHolder.get()
    );
    SimpleTimeSeries row = simpleTimeSeriesView.getRow(0);
    Assert.assertNull(row);
  }

  @Test
  public void testMultipleNull() throws Exception
  {
    SimpleTimeSeriesColumnValueSelector selector = new SimpleTimeSeriesColumnValueSelector(null);
    int rowCount = 10000;

    for (int i = 0; i < rowCount; i++) {
      columnSerializer.serialize(selector);
    }

    ByteBuffer byteBuffer = serializeToByteBuffer(columnSerializer);
    Assert.assertEquals(374, columnSerializer.getSerializedSize());

    SimpleTimeSeriesView simpleTimeSeriesView = SimpleTimeSeriesView.create(byteBuffer,
                                                                            rowIndexUncompressedBlockHolder.get(),
                                                                            dataUncompressedBlockHolder.get()
    );
    for (int i = 0; i < rowCount; i++) {
      SimpleTimeSeries row = simpleTimeSeriesView.getRow(0);
      Assert.assertNull(row);
    }
  }

  private SimpleTimeSeriesView createSimpleTimeSeriesView() throws IOException
  {
    ByteBuffer masterByteBuffer = serializeToByteBuffer(columnSerializer);
    SimpleTimeSeriesView simpleTimeSeriesView = SimpleTimeSeriesView.create(
        masterByteBuffer,
        rowIndexUncompressedBlockHolder.get(),
        dataUncompressedBlockHolder.get()
    );
    return simpleTimeSeriesView;
  }

  @Nonnull
  private static ByteBuffer serializeToByteBuffer(SimpleTimeSeriesColumnSerializer columnSerializer) throws IOException
  {
    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();

    columnSerializer.getSerializedSize();
    columnSerializer.writeTo(channel, null);
    ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size()).order(ByteOrder.nativeOrder());
    channel.readFully(0, byteBuffer);
    byteBuffer.flip();
    Assert.assertEquals(columnSerializer.getSerializedSize(), byteBuffer.limit());

    return byteBuffer;
  }

  private static SimpleTimeSeries buildTimeSeries(DateTime start, int numDataPoints, int offset)
  {
    SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
        SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW,
        Integer.MAX_VALUE
    );

    for (int i = offset; i < offset + numDataPoints; i++) {
      simpleTimeSeries.addDataPoint(start.plusMillis(i).getMillis(), i);
    }

    return simpleTimeSeries;
  }

  private static class SimpleTimeSeriesColumnValueSelector implements ColumnValueSelector<SimpleTimeSeries>
  {
    private final SimpleTimeSeries simpleTimeSeries;

    public SimpleTimeSeriesColumnValueSelector(SimpleTimeSeries simpleTimeSeries)
    {
      this.simpleTimeSeries = simpleTimeSeries;
    }

    @Override
    public double getDouble()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public boolean isNull()
    {
      return false;
    }

    @Nullable
    @Override
    public SimpleTimeSeries getObject()
    {
      return simpleTimeSeries;
    }

    @Override
    public Class<? extends SimpleTimeSeries> classOfObject()
    {
      return SimpleTimeSeries.class;
    }
  }
}
