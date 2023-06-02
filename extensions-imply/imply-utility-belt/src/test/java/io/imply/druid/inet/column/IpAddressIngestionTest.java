/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.segment.virtual.IpAddressFormatVirtualColumn;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IpAddressIngestionTest extends InitializedNullHandlingTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  Closer closer;

  @Before
  public void setup()
  {
    closer = Closer.create();
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }


  public IpAddressIngestionTest()
  {
    IpAddressModule.registerHandlersAndSerde();
    List<? extends Module> mods = IpAddressTestUtils.LICENSED_IP_ADDRESS_MODULE.getJacksonModules();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
  }

  @Test
  public void testIngestIpAndScanSegments() throws Exception
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
    List<Segment> segs = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanSegmentsWithIpStringify() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(new IpAddressFormatVirtualColumn(
                                                 "v0",
                                                 "ipv4",
                                                 true,
                                                 false
                                             ))
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpWithMergesAndScanSegments() throws Exception
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
    List<Segment> segs = IpAddressTestUtils.createIpAddressSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        5
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpWithMergesAndScanSegmentsUncompressed() throws Exception
  {
    // sanity check test to make sure uncompressed works (in case some intermediary config is set to write
    // out uncompressed dimensions (such as MSQ at the time of this test being written)
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        "simple_ip_test_data.tsv",
        new DelimitedInputFormat(
            Arrays.asList("timestamp", "ipv4", "ipv6", "ipmix"),
            null,
            null,
            false,
            false,
            0
        ),
        new TimestampSpec("timestamp", null, null),
        DimensionsSpec.builder()
                      .setDimensions(
                          Arrays.asList(
                              new IpAddressDimensionSchema("ipv4", true),
                              new IpAddressDimensionSchema("ipv6", true),
                              new IpAddressDimensionSchema("ipmix", true)
                          )
                      )
                      .build(),
        null,
        new AggregatorFactory[0],
        Granularities.NONE,
        false,
        IndexSpec.builder().withDimensionCompression(CompressionStrategy.UNCOMPRESSED).build()
    );

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
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpWithMergesAndQueryUsingBitmap() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                                             )
                                             .filters(
                                                 new SearchQueryDimFilter("v0", new ContainsSearchQuerySpec("10.10", true), null)
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = IpAddressTestUtils.createIpAddressSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        3
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(2, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanSegmentsRollup() throws Exception
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

    List<Segment> segs = IpAddressTestUtils.createIpAddressDefaultDaySegments(helper, tempFolder);
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(10, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanSegmentsAndFilter() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .filters(new SelectorDimFilter("ipv4", "22.22.23.24", null))
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    // bitmap index isn't supported in this context since it needs strings and selector doesn't know what to do with ip
    // address or how to compare them so predicate is out
    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testIngestIpAndScanSegmentsAndFilterRange() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .filters(
                                                 new BoundDimFilter(
                                                     "ipv4",
                                                     "11.11.11.11",
                                                     "55.55.55.55",
                                                     null,
                                                     null,
                                                     null,
                                                     null,
                                                     null
                                                 )
                                             )
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();

    List<Segment> segs = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);


    List<ScanResultValue> results = seq.toList();
    // bitmap index isn't supported in this context since it needs strings and bound doesn't know what to do with ip
    // address or how to compare them so predicate is out
    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testIngestIpAndScanIncrementalIndex() throws Exception
  {
    Segment index = IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex();

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

    Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(index),
        scanQuery
    );
    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanIncrementalIndexWithIpStringify() throws Exception
  {
    Segment index = IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex();
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(new IpAddressFormatVirtualColumn(
                                                 "v0",
                                                 "ipv4",
                                                 true,
                                                 false
                                             ))
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(index),
        scanQuery
    );
    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(11, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanIncrementalIndexRollup() throws Exception
  {
    Segment index = IpAddressTestUtils.createIpAddressDefaultDailyIncrementalIndex();
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

    Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(index),
        scanQuery
    );
    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(10, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanIncrementalIndexAndFilter() throws Exception
  {
    Segment index = IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex();
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .filters(new SelectorDimFilter("ipv4", "22.22.23.24", null))
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();

    Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(index),
        scanQuery
    );
    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(2, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestIpAndScanIncrementalIndexAndFilterRangeIncrementalIndex() throws Exception
  {
    Segment index = IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex();
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .filters(
                                                 new BoundDimFilter(
                                                     "ipv4",
                                                     "11.11.11.11",
                                                     "55.55.55.55",
                                                     null,
                                                     null,
                                                     null,
                                                     null,
                                                     null
                                                 )
                                             )
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();

    Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(index),
        scanQuery
    );
    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());

    // lol the bound filter spits out nonsense since it is using lexicographic comparator when using the predicate
    // filter, unlike when using the bitmap index which just seems to use the comparator of the underlying dictionary
    // to check the range. this seems to be an assumption about the type of data and nature of dictionary encoded
    // column that BoundFilter is assuming, we might do better to replace it with a different range filter that just
    // uses the natural comparator for a given type (or maybe we could do this for bound filter but it seems pretty
    // tied into how string dictionary encoded columns work...)
    Assert.assertEquals(3, ((List) results.get(0).getEvents()).size());
  }
}
