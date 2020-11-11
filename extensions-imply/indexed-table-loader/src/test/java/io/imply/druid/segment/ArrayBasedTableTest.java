/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.ArrayBasedIndexedTableDruidModule;
import io.imply.druid.segment.join.IndexedTableManager;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ArrayBasedTableTest
{
  private static final String STRING_COL_1 = "market";
  private static final String LONG_COL_1 = "longNumericNull";
  private static final String DOUBLE_COL_1 = "doubleNumericNull";
  private static final String FLOAT_COL_1 = "floatNumericNull";
  private static final String STRING_COL_2 = "partial_null_column";
  private static final String MULTI_VALUE_COLUMN = "placementish";
  private static final String DIM_NOT_EXISTS = "DIM_NOT_EXISTS";
  private static final String DATASOURCE = "DATASOURCE";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ArrayBasedTable target;
  private List<String> columnNames;
  private IntArrayList row0;

  @Before
  public void setup() throws IOException, SegmentLoadingException
  {
    NullHandling.initializeForTests();
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(new SegmentizerModule());
    mapper.registerModules(new ArrayBasedIndexedTableDruidModule().getJacksonModules());
    final IndexIO indexIO = new IndexIO(mapper, () -> 0);
    final IndexedTableManager manager = new IndexedTableManager();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(IndexIO.class, indexIO)
            .addValue(IndexedTableManager.class, manager)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
    );

    final IndexMerger indexMerger =
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
    Interval testInterval = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");
    File segment = new File(temporaryFolder.newFolder(), "segment");
    File persisted = indexMerger.persist(
        data,
        testInterval,
        segment,
        new IndexSpec(),
        null
    );
    File factoryJson = new File(persisted, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    SegmentizerFactory factory = mapper.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof MMappedQueryableSegmentizerFactory);

    DataSegment dataSegment = new DataSegment(
        DATASOURCE,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        segment.getTotalSpace()
    );
    QueryableIndexSegment loaded = (QueryableIndexSegment) factory.factorize(dataSegment, segment, false);

    columnNames = ImmutableList.<String>builder().add(ColumnHolder.TIME_COLUMN_NAME)
                                                 .addAll(loaded.asQueryableIndex().getColumnNames()).build();
    target = new ArrayBasedTable(DATASOURCE, loaded);
    row0 = new IntArrayList(new int[columnNames.size()]);
  }

  @Test
  public void testInitShouldGenerateCorrectTable()
  {
    Assert.assertEquals(1209, target.getEncodedRows().size());
  }

  @Test
  public void testGetRowAdapterShouldReturnCorrectTimestampAsLongMillis()
  {
    long ts = target.getRowAdapter().timestampFunction().applyAsLong(row0);
    Assert.assertEquals(1294790400000L, ts);
  }

  @Test
  public void testGetRowAdapterStringColumn()
  {
    Object stringVal = target.getRowAdapter().columnFunction(STRING_COL_1).apply(row0);
    Assert.assertEquals("spot", stringVal);
    row0.set(columnNames.indexOf(STRING_COL_1), 1);
    stringVal = target.getRowAdapter().columnFunction(STRING_COL_1).apply(row0);
    Assert.assertEquals("total_market", stringVal);
    row0.set(columnNames.indexOf(STRING_COL_1), 2);
    stringVal = target.getRowAdapter().columnFunction(STRING_COL_1).apply(row0);
    Assert.assertEquals("upfront", stringVal);
  }

  @Test
  public void testGetRowAdapterNullableStringColumn()
  {
    Object stringVal = target.getRowAdapter().columnFunction(STRING_COL_2).apply(row0);
    Assert.assertEquals(null, stringVal);
    row0.set(columnNames.indexOf(STRING_COL_2), 1);
    stringVal = target.getRowAdapter().columnFunction(STRING_COL_2).apply(row0);
    Assert.assertEquals("value", stringVal);
    row0.set(columnNames.indexOf(STRING_COL_2), 2);
    stringVal = target.getRowAdapter().columnFunction(STRING_COL_2).apply(row0);
    Assert.assertEquals(null, stringVal);
  }

  @Test
  public void testGetRowAdapterMultiValueStringColumn()
  {
    Object stringVal = target.getRowAdapter().columnFunction(MULTI_VALUE_COLUMN).apply(row0);
    Assert.assertEquals(null, stringVal);
    row0.set(columnNames.indexOf(MULTI_VALUE_COLUMN), 1);
    stringVal = target.getRowAdapter().columnFunction(MULTI_VALUE_COLUMN).apply(row0);
    Assert.assertEquals(null, stringVal);
    row0.set(columnNames.indexOf(MULTI_VALUE_COLUMN), 2);
    stringVal = target.getRowAdapter().columnFunction(MULTI_VALUE_COLUMN).apply(row0);
    Assert.assertEquals(null, stringVal);
  }

  @Test
  public void testGetRowAdapterNullableLongColumn()
  {
    Object val = target.getRowAdapter().columnFunction(LONG_COL_1).apply(row0);
    Assert.assertEquals(10L, val);
    row0.set(columnNames.indexOf(LONG_COL_1), 1);
    val = target.getRowAdapter().columnFunction(LONG_COL_1).apply(row0);
    Assert.assertEquals(20L, val);
    row0.set(columnNames.indexOf(LONG_COL_1), 2);
    val = target.getRowAdapter().columnFunction(LONG_COL_1).apply(row0);
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0L : null, val);
  }

  @Test
  public void testGetRowAdapterNullableFloatColumn()
  {
    Object val = target.getRowAdapter().columnFunction(FLOAT_COL_1).apply(row0);
    Assert.assertEquals(10.0f, val);
    row0.set(columnNames.indexOf(FLOAT_COL_1), 1);
    val = target.getRowAdapter().columnFunction(FLOAT_COL_1).apply(row0);
    Assert.assertEquals(20.0f, val);
    row0.set(columnNames.indexOf(FLOAT_COL_1), 2);
    val = target.getRowAdapter().columnFunction(FLOAT_COL_1).apply(row0);
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.0f : null, val);
  }

  @Test
  public void testGetRowAdapterNullableDoubleColumn()
  {
    Object val = target.getRowAdapter().columnFunction(DOUBLE_COL_1).apply(row0);
    Assert.assertEquals(10.0, val);
    row0.set(columnNames.indexOf(DOUBLE_COL_1), 1);
    val = target.getRowAdapter().columnFunction(DOUBLE_COL_1).apply(row0);
    Assert.assertEquals(20.0, val);
    row0.set(columnNames.indexOf(DOUBLE_COL_1), 2);
    val = target.getRowAdapter().columnFunction(DOUBLE_COL_1).apply(row0);
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.0 : null, val);
  }

  @Test
  public void testGetRowAdapterColumnDoesNotExistsShouldReturnNull()
  {
    Object dim1Row0 = target.getRowAdapter().columnFunction(DIM_NOT_EXISTS).apply(row0);
    Assert.assertNull(dim1Row0);
  }
}
