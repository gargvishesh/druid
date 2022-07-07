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
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
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
  private SimpleTimeSeriesComplexMetricSerde complexMetricSerde;


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
    complexMetricSerde = new SimpleTimeSeriesComplexMetricSerde();
    columnSerializer = (SimpleTimeSeriesColumnSerializer) complexMetricSerde.getSerializer(writeOutMedium, "unused");
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
    Assert.assertEquals(901, columnSerializer.getSerializedSize());

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      SimpleTimeSeries row1 = getSimpleTimeSeries(complexColumn, 0);
      SimpleTimeSeries row2 = getSimpleTimeSeries(complexColumn, 1);
      Assert.assertEquals(SIMPLE_TIME_SERIES_1.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
      Assert.assertEquals(SIMPLE_TIME_SERIES_2.asSimpleTimeSeriesData(), row2.asSimpleTimeSeriesData());
    }
  }

  @Test
  public void testEnd2EndSimpleWithNull() throws Exception
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(null));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));
    //update if size is intended to change
    Assert.assertEquals(901, columnSerializer.getSerializedSize());

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      SimpleTimeSeries row1 = getSimpleTimeSeries(complexColumn, 0);
      SimpleTimeSeries row2 = getSimpleTimeSeries(complexColumn, 1);
      SimpleTimeSeries row3 = getSimpleTimeSeries(complexColumn, 2);
      Assert.assertEquals(SIMPLE_TIME_SERIES_1.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
      Assert.assertNull(row2);
      Assert.assertEquals(SIMPLE_TIME_SERIES_2.asSimpleTimeSeriesData(), row3.asSimpleTimeSeriesData());
    }
  }

  @Test
  public void testEnd2EndMultiBlock() throws Exception
  {
    int numberOfRows = 5000;
    int entriesPerRow = 10;
    List<SimpleTimeSeries> sourceRowsList = SimpleTimeSeriesTestUtil.buildTimeSeriesList(
        numberOfRows,
        entriesPerRow,
        simpleTimeSeries -> columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries))
    );
    //update if size is intended to change (note: > 64k block size means we are testing cross-block reads
    long serializedSize = columnSerializer.getSerializedSize();
    Assert.assertTrue(serializedSize > 2 * CompressedPools.BUFFER_SIZE);

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      for (int i = 0; i < numberOfRows; i++) {
        SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, i);
        Assert.assertNotNull(row);
        Assert.assertEquals(sourceRowsList.get(i).asSimpleTimeSeriesData(), row.asSimpleTimeSeriesData());
      }
    }

  }

  @Test
  public void testMultiBlockSpanRow() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 256 * 1024, 0);
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));

    long serializedSize = columnSerializer.getSerializedSize();

    Assert.assertTrue(serializedSize > 2 * CompressedPools.BUFFER_SIZE);

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      SimpleTimeSeries row1 = getSimpleTimeSeries(complexColumn, 0);

      Assert.assertEquals(simpleTimeSeries.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
    }
  }

  @Test
  public void testEmptyTimeSeries() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 0, 0);
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, 0);

      Assert.assertNull(row);
    }
  }

  @Test
  public void testEmptyTimeSeriesInterleaved() throws Exception
  {
    SimpleTimeSeries simpleTimeSeries = buildTimeSeries(START_DATE_TIME, 0, 0);

    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(simpleTimeSeries));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, 1);

      // 0-element rows are encoded as null
      Assert.assertNull(row);
    }
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

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn()) {
      for (int i = 0; i < rowCount; i++) {
        SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, i);
        Assert.assertEquals(input.get(i).asSimpleTimeSeriesData(), row.asSimpleTimeSeriesData());
      }
    }
  }

  @Test
  public void testSingleNull() throws Exception
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(null));

    ByteBuffer byteBuffer = serializeToByteBuffer(columnSerializer);
    Assert.assertEquals(58, columnSerializer.getSerializedSize());

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn(byteBuffer)) {
      SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, 0);

      Assert.assertNull(row);
    }
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
    Assert.assertEquals(386, columnSerializer.getSerializedSize());

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn(byteBuffer)) {
      for (int i = 0; i < rowCount; i++) {
        SimpleTimeSeries row = getSimpleTimeSeries(complexColumn, i);
        Assert.assertNull(row);
      }
    }
  }

  @Test
  public void testWriteToWithoutGetSerializedSize() throws IOException
  {
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_1));
    columnSerializer.serialize(new SimpleTimeSeriesColumnValueSelector(SIMPLE_TIME_SERIES_2));

    try (SimpleTimeSeriesComplexColumn complexColumn = createSimpleTimeSeriesComplexColumn(false)) {
      SimpleTimeSeries row1 = getSimpleTimeSeries(complexColumn, 0);
      SimpleTimeSeries row2 = getSimpleTimeSeries(complexColumn, 1);
      Assert.assertEquals(SIMPLE_TIME_SERIES_1.asSimpleTimeSeriesData(), row1.asSimpleTimeSeriesData());
      Assert.assertEquals(SIMPLE_TIME_SERIES_2.asSimpleTimeSeriesData(), row2.asSimpleTimeSeriesData());
    }

  }

  private static SimpleTimeSeries getSimpleTimeSeries(SimpleTimeSeriesComplexColumn complexColumn, int rowNum)
  {
    return ((SimpleTimeSeriesContainer) complexColumn.getRowValue(rowNum)).getSimpleTimeSeries();
  }

  private SimpleTimeSeriesComplexColumn createSimpleTimeSeriesComplexColumn() throws IOException
  {
    return createSimpleTimeSeriesComplexColumn(true);
  }

  private SimpleTimeSeriesComplexColumn createSimpleTimeSeriesComplexColumn(boolean checkSize) throws IOException
  {
    ByteBuffer masterByteBuffer = serializeToByteBuffer(columnSerializer, checkSize);

    return createSimpleTimeSeriesComplexColumn(masterByteBuffer);
  }

  private SimpleTimeSeriesComplexColumn createSimpleTimeSeriesComplexColumn(ByteBuffer byteBuffer)
  {
    ColumnBuilder builder = new ColumnBuilder();
    complexMetricSerde.deserializeColumn(byteBuffer, builder);
    // TODO: what all values of the builder must be set? (valueType IS needed, others don't seem to matter)
    builder.setType(ValueType.COMPLEX);

    ColumnHolder columnHolder = builder.build();

    SimpleTimeSeriesComplexColumn column = (SimpleTimeSeriesComplexColumn) columnHolder.getColumn();

    return column;
  }

  @Nonnull
  private static ByteBuffer serializeToByteBuffer(SimpleTimeSeriesColumnSerializer columnSerializer) throws IOException
  {
    return serializeToByteBuffer(columnSerializer, true);
  }

  @Nonnull
  private static ByteBuffer serializeToByteBuffer(SimpleTimeSeriesColumnSerializer columnSerializer, boolean checkSize)
      throws IOException
  {
    HeapByteBufferWriteOutBytes channel = new HeapByteBufferWriteOutBytes();

    if (checkSize) {
      columnSerializer.getSerializedSize();
    }

    columnSerializer.writeTo(channel, null);

    ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size()).order(ByteOrder.nativeOrder());

    channel.readFully(0, byteBuffer);
    byteBuffer.flip();

    if (checkSize) {
      Assert.assertEquals(columnSerializer.getSerializedSize(), byteBuffer.limit());
    }

    return byteBuffer;
  }

  private static SimpleTimeSeries buildTimeSeries(DateTime start, int numDataPoints, int offset)
  {
    SimpleTimeSeries simpleTimeSeries =
        new SimpleTimeSeries(SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW, Integer.MAX_VALUE);

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
