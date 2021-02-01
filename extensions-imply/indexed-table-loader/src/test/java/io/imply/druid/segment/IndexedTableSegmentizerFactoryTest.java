/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.ArrayBasedIndexedTableDruidModule;
import io.imply.druid.segment.join.CloseTableFirstReferenceCountingIndexedTable;
import io.imply.druid.segment.join.IndexedTableManager;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class IndexedTableSegmentizerFactoryTest
{
  private static final String TABLE_NAME = "test";
  private static final String STRING_COL_1 = "market";
  private static final String LONG_COL_1 = "longNumericNull";
  private static final String DOUBLE_COL_1 = "doubleNumericNull";
  private static final String FLOAT_COL_1 = "floatNumericNull";
  private static final String STRING_COL_2 = "partial_null_column";

  private static final Set<String> KEY_COLUMNS =
      ImmutableSet.of(STRING_COL_1, LONG_COL_1, FLOAT_COL_1, DOUBLE_COL_1, STRING_COL_2);

  private IndexedTableManager indexedTableManager;
  private SegmentizerFactory factory;
  private Interval testInterval;
  private List<String> columnNames;
  private File persistedSegmentRoot;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() throws IOException
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

    indexedTableManager = manager;
    IndexMerger indexMerger = new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    SegmentizerFactory expectedFactory = new OnHeapIndexedTableSegmentizerFactory(
        indexIO,
        indexedTableManager,
        KEY_COLUMNS
    );
    testInterval = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");

    columnNames = data.getColumnNames();
    File segment = new File(temporaryFolder.newFolder(), "segment");
    persistedSegmentRoot = indexMerger.persist(
        data,
        testInterval,
        segment,
        new IndexSpec(
            null,
            null,
            null,
            null,
            expectedFactory
        ),
        null
    );

    File factoryJson = new File(persistedSegmentRoot, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    factory = mapper.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof OnHeapIndexedTableSegmentizerFactory);
    Assert.assertEquals(expectedFactory, factory);

    assertEmptyTable();
  }

  @Test
  public void testIndexedTableSegmentizerLoadAndClose() throws IOException, SegmentLoadingException
  {
    // load a segment
    final DataSegment dataSegment = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegment,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // unload the segment
    loaded.close();
    assertEmptyTable();
  }

  @Test
  public void testIndexedTableSegmentizerLoadAndUpdate() throws IOException, SegmentLoadingException
  {
    // load a segment
    final DataSegment dataSegment = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegment,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // load newer segment, expect to replace old segment
    final DataSegment dataSegmentNewer = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loadedNewer = factory.factorize(
        dataSegmentNewer,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loadedNewer);

    // close old segment does not drop new segment or table
    loaded.close();
    assertIndexedTableExpectedSegmentLoaded(loadedNewer);
  }

  @Test
  public void testIndexedTableSegmentizerLoadAndUpdateDoesNotReplaceWithOlderVersion()
      throws IOException, SegmentLoadingException
  {
    // load newer segment
    final DataSegment dataSegment = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegment,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);
    final IndexedTable table = indexedTableManager.getIndexedTable(TABLE_NAME);

    // load older segment
    final DataSegment dataSegmentOlder = new DataSegment(
        TABLE_NAME,
        testInterval,
        DateTimes.nowUtc().minusDays(1).toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loadedOlder = factory.factorize(
        dataSegmentOlder,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // expect same object since expect do nothing
    Assert.assertTrue(table == indexedTableManager.getIndexedTable(TABLE_NAME));

    // close older segment does not drop table
    loadedOlder.close();
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // expect same object since expect do nothing
    Assert.assertTrue(table == indexedTableManager.getIndexedTable(TABLE_NAME));
  }

  @Test
  public void testIndexedTableSegmentizerLoadReloadSameVersionForSomeReasonButDoesNothing()
      throws IOException, SegmentLoadingException
  {
    final String version = DateTimes.nowUtc().toString();
    final DataSegment dataSegment = new DataSegment(
        TABLE_NAME,
        testInterval,
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        persistedSegmentRoot.getTotalSpace()
    );

    // load segment
    final Segment loaded = factory.factorize(
        dataSegment,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);
    final IndexedTable table = indexedTableManager.getIndexedTable(TABLE_NAME);

    // load segment again somehow, expect to do nothing
    final Segment reloaded = factory.factorize(
        dataSegment,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);
    // expect same object since expect do nothing
    Assert.assertTrue(table == indexedTableManager.getIndexedTable(TABLE_NAME));

    // close for fun
    loaded.close();
    assertEmptyTable();
  }

  @Test
  public void testIndexedTableSegmentizerLoadAnotherSegmentsWithSameVersionDropsTable()
      throws SegmentLoadingException, IOException
  {
    final String version = DateTimes.nowUtc().toString();

    // load first segment
    final DataSegment dataSegmentFirst = new DataSegment(
        TABLE_NAME,
        testInterval,
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegmentFirst,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);
    final IndexedTable table = indexedTableManager.getIndexedTable(TABLE_NAME);

    // load second segment of same version and interval from different partition will drop the table
    final DataSegment dataSegmentSecond = new DataSegment(
        TABLE_NAME,
        testInterval,
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(1, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment partition = factory.factorize(
        dataSegmentSecond,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    Assert.assertTrue(partition instanceof QueryableIndexSegment);
    assertEmptyTable();

    // ensure no ill effect from closing previously loaded segment (which will try to drop the table)
    loaded.close();
    assertEmptyTable();
  }

  @Test
  public void testIndexedTableSegmentizerLoadAnotherSegmentsWithDifferentEntryDropsTable()
      throws SegmentLoadingException, IOException
  {
    final String version = DateTimes.nowUtc().toString();

    // load first segment
    final DataSegment dataSegmentFirst = new DataSegment(
        TABLE_NAME,
        testInterval,
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegmentFirst,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // loading a segment from a different interval will drop the table
    final DataSegment dataSegmentSecond = new DataSegment(
        TABLE_NAME,
        Intervals.of("2011-05-2T00:00:00.000Z/2011-08-01T00:00:00.000Z"),
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment different = factory.factorize(
        dataSegmentSecond,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    Assert.assertTrue(different instanceof QueryableIndexSegment);
    assertEmptyTable();

    // loading a segment from a different interval will be blacklisted and not reload the table
    final DataSegment dataSegmentThird = new DataSegment(
        TABLE_NAME,
        Intervals.of("2011-08-2T00:00:00.000Z/2011-11-01T00:00:00.000Z"),
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment different3 = factory.factorize(
        dataSegmentThird,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    Assert.assertTrue(different3 instanceof QueryableIndexSegment);
    assertEmptyTable();


    // ensure no ill effect from closing previously loaded segment (which will try to drop the table)
    loaded.close();
    assertEmptyTable();
  }

  @Test
  public void testIndexedTableSegmentizerLoadAnotherSegmentsWithDifferentEntryDropsTableButCanLaterClearBlacklist()
      throws SegmentLoadingException, IOException
  {
    final String version = DateTimes.nowUtc().toString();

    // load first segment
    final DataSegment dataSegmentFirst = new DataSegment(
        TABLE_NAME,
        testInterval,
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loaded = factory.factorize(
        dataSegmentFirst,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loaded);

    // loading a segment from a different interval will drop the table
    final DataSegment dataSegmentSecond = new DataSegment(
        TABLE_NAME,
        Intervals.of("2011-05-2T00:00:00.000Z/2011-08-01T00:00:00.000Z"),
        version,
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment different = factory.factorize(
        dataSegmentSecond,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    Assert.assertTrue(different instanceof QueryableIndexSegment);
    assertEmptyTable();

    // loading a segment from a different interval will clear the blacklist
    final DataSegment dataSegmentThird = new DataSegment(
        TABLE_NAME,
        Intervals.of("2011-01-12T00:00:00.000Z/2011-11-01T00:00:00.000Z"),
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        new NumberedShardSpec(0, 2),
        null,
        persistedSegmentRoot.getTotalSpace()
    );
    final Segment loadedNewer = factory.factorize(
        dataSegmentThird,
        persistedSegmentRoot,
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    assertIndexedTableExpectedSegmentLoaded(loadedNewer);

    // ensure no ill effect from closing previously loaded segment (which will try to drop the table)
    loaded.close();
    different.close();

    assertIndexedTableExpectedSegmentLoaded(loadedNewer);
  }


  private void assertEmptyTable()
  {
    Assert.assertEquals(0, indexedTableManager.getIndexedTables().size());
    Assert.assertNull(indexedTableManager.getIndexedTable(TABLE_NAME));
  }

  private void assertIndexedTableExpectedSegmentLoaded(Segment segment)
  {
    Assert.assertTrue(segment instanceof IndexedTableSegment);
    Assert.assertEquals(1, indexedTableManager.getIndexedTables().size());
    IndexedTable table = indexedTableManager.getIndexedTable(TABLE_NAME);
    Assert.assertEquals(segment.getId().getVersion(), table.version());
    Assert.assertNotNull(table);
    Assert.assertTrue(table instanceof CloseTableFirstReferenceCountingIndexedTable);

    Assert.assertEquals(1209, table.numRows());
    Assert.assertEquals(KEY_COLUMNS, table.keyColumns());
    Assert.assertEquals(20, table.rowSignature().size());

    Assert.assertTrue(table.isCacheable());
    Assert.assertArrayEquals(
        OnHeapIndexedTableSegmentizerFactory.computeCacheKey(segment.getId()),
        table.computeCacheKey()
    );

    // read time column
    Assert.assertEquals(1294790400000L, table.columnReader(0).read(0));

    final int marketColumnIndex = table.rowSignature().indexOf("market");
    // read market column
    Assert.assertEquals("spot", table.columnReader(marketColumnIndex).read(0));
    // expect explosion trying to get index for time column since isn't key column
    try {
      table.columnIndex(0);
      // should explode
      Assert.assertTrue(false);
    }
    catch (IAE e) {
      Assert.assertEquals("Column[0] is not a key column", e.getMessage());
    }

    // get index for market column, is key column, expect there to be a bunch of rows that match
    Assert.assertEquals(837, table.columnIndex(marketColumnIndex).find("spot").size());

    // get index for market column, is key column, expect there to be a empty list for value that isn't in the table
    Assert.assertEquals(IntLists.EMPTY_LIST, table.columnIndex(marketColumnIndex).find("not in the table"));
  }
}
