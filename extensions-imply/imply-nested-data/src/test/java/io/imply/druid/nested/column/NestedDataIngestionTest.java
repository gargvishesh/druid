/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.nested.NestedDataModule;
import io.imply.druid.nested.virtual.NestedFieldVirtualColumn;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;

public class NestedDataIngestionTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataColumnSerializer.class);
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public NestedDataIngestionTest()
  {
    NestedDataModule.registerHandlersAndSerde();
    List<? extends Module> mods = NestedDataModule.getJacksonModulesList();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
  }

  @Test
  public void testIngestAndScanSegments() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", ".x", "x"),
                                                 new NestedFieldVirtualColumn("nester", ".x.[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", ".y.c.[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestAndScanSegmentsRealtime() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", ".x", "x"),
                                                 new NestedFieldVirtualColumn("nester", ".x.[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", ".y.c.[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createDefaultHourlyIncrementalIndex()
    );
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder);


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsSegments.get(0).getEvents(), resultsRealtime.get(0).getEvents());
  }

  @Test
  public void testIngestAndScanSegmentsTsv() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", ".x", "x"),
                                                 new NestedFieldVirtualColumn("nester", ".x.[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", ".y.c.[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegmentsTsv(helper, tempFolder);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestWithMergesAndScanSegments() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        3
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestAndScanSegmentsAndFilter() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", ".x", "x")
                                             )
                                             .filters(
                                                 new SelectorDimFilter("x", "200", null)
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(1, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  private static void logResults(List<ScanResultValue> results)
  {
    StringBuilder bob = new StringBuilder();
    for (Object event : (List) results.get(0).getEvents()) {
      bob.append("[").append(event).append("]").append("\n");
    }
    LOG.info("results:\n%s", bob);
  }
}
