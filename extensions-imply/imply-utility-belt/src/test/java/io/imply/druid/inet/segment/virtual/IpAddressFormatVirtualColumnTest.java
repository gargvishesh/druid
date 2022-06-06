/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.segment.virtual;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.column.IpAddressTestUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IpAddressFormatVirtualColumnTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper helper;

  public IpAddressFormatVirtualColumnTest()
  {
    IpAddressModule.registerHandlersAndSerde();
    List<? extends Module> mods = IpAddressTestUtils.LICENSED_IP_ADDRESS_MODULE.getJacksonModules();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn("v0", "column", true, false);
    IpAddressFormatVirtualColumn roundTrip = MAPPER.readValue(
        MAPPER.writeValueAsString(virtualColumn),
        IpAddressFormatVirtualColumn.class
    );
    Assert.assertEquals(virtualColumn, roundTrip);
    Assert.assertArrayEquals(virtualColumn.getCacheKey(), roundTrip.getCacheKey());
  }

  @Test
  public void testSerdeDefaults() throws JsonProcessingException
  {
    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn("v0", "column", null, null);
    IpAddressFormatVirtualColumn roundTrip = MAPPER.readValue(
        MAPPER.writeValueAsString(virtualColumn),
        IpAddressFormatVirtualColumn.class
    );
    Assert.assertEquals(virtualColumn, roundTrip);
    Assert.assertArrayEquals(virtualColumn.getCacheKey(), roundTrip.getCacheKey());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(IpAddressFormatVirtualColumn.class)
                  .withNonnullFields("name", "field")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testDimensionSelector() throws Exception
  {
    List<Segment> segments = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);

    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn(
        "stringifyV4",
        "ipv4",
        true,
        false
    );

    QueryableIndex index = segments.get(0).asQueryableIndex();
    SimpleAscendingOffset offset = new SimpleAscendingOffset(index.getNumRows());

    Closer closer = Closer.create();
    DimensionSelector selector = virtualColumn.makeDimensionSelector(
        DefaultDimensionSpec.of("ipv4"), new ColumnCache(index, closer), offset
    );
    Assert.assertTrue(selector.nameLookupPossibleInAdvance());
    Assert.assertEquals(6, selector.getValueCardinality());
    List<Object> results = new ArrayList<>();
    while (offset.withinBounds()) {
      IndexedInts row = selector.getRow();
      results.add(selector.lookupName(row.get(0)));
      offset.increment();
    }
    Assert.assertEquals(
        Arrays.asList(
            "1.2.3.4",
            "5.6.7.8",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12",
            null,
            "1.2.3.4",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12"
        ),
        results
    );
    closer.close();
  }

  @Test
  public void testDimensionSelectorWithColumnSelectorFactory() throws Exception
  {
    Segment segment = IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex();

    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn(
        "stringifyV4",
        "ipv4",
        true,
        false
    );

    StorageAdapter storageAdapter = segment.asStorageAdapter();
    Cursor cursor = storageAdapter.makeCursors(null, storageAdapter.getInterval(), VirtualColumns.EMPTY, Granularities.ALL, false, null)
                                  .toList()
                                  .get(0);
    ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

    DimensionSelector selector = virtualColumn.makeDimensionSelector(DefaultDimensionSpec.of("ipv4"), columnSelectorFactory);
    Assert.assertTrue(selector.nameLookupPossibleInAdvance());
    Assert.assertEquals(6, selector.getValueCardinality());
    List<Object> results = new ArrayList<>();
    while (!cursor.isDone()) {
      IndexedInts row = selector.getRow();
      results.add(selector.lookupName(row.get(0)));
      cursor.advance();
    }
    Assert.assertEquals(
        Arrays.asList(
            "1.2.3.4",
            "5.6.7.8",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12",
            null,
            "1.2.3.4",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12"
        ),
        results
    );
  }

  @Test
  public void testSingleValueDimensionVectorSelector() throws Exception
  {
    List<Segment> segments = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);

    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn(
        "stringifyV4",
        "ipv4",
        true,
        false
    );

    QueryableIndex index = segments.get(0).asQueryableIndex();
    VectorOffset offset = new NoFilterVectorOffset(4, 0, index.getNumRows());

    Closer closer = Closer.create();
    SingleValueDimensionVectorSelector selector = virtualColumn.makeSingleValueVectorDimensionSelector(
        DefaultDimensionSpec.of("ipv4"),
        new ColumnCache(index, closer),
        offset
    );
    Assert.assertEquals(offset.getMaxVectorSize(), selector.getMaxVectorSize());
    Assert.assertTrue(selector.nameLookupPossibleInAdvance());
    Assert.assertEquals(6, selector.getValueCardinality());
    List<Object> results = new ArrayList<>();
    while (!offset.isDone()) {
      int[] vector = selector.getRowVector();
      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        results.add(selector.lookupName(vector[i]));
      }
      Assert.assertEquals(offset.getCurrentVectorSize(), selector.getCurrentVectorSize());

      offset.advance();
    }
    Assert.assertEquals(
        Arrays.asList(
            "1.2.3.4",
            "5.6.7.8",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12",
            null,
            "1.2.3.4",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12"
        ),
        results
    );
    closer.close();
  }

  @Test
  public void testVectorObjectSelector() throws Exception
  {
    List<Segment> segments = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);

    IpAddressFormatVirtualColumn virtualColumn = new IpAddressFormatVirtualColumn(
        "stringifyV4",
        "ipv4",
        true,
        false
    );

    Closer closer = Closer.create();
    QueryableIndex index = segments.get(0).asQueryableIndex();
    VectorOffset offset = new NoFilterVectorOffset(4, 0, index.getNumRows());
    VectorObjectSelector selector = virtualColumn.makeVectorObjectSelector(
        "ipv4",
        new ColumnCache(index, closer),
        offset
    );
    Assert.assertEquals(offset.getMaxVectorSize(), selector.getMaxVectorSize());

    List<Object> results = new ArrayList<>();
    while (!offset.isDone()) {
      Object[] vector = selector.getObjectVector();
      for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
        results.add(vector[i]);
      }
      Assert.assertEquals(offset.getCurrentVectorSize(), selector.getCurrentVectorSize());
      offset.advance();
    }
    Assert.assertEquals(
        Arrays.asList(
            "1.2.3.4",
            "5.6.7.8",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12",
            null,
            "1.2.3.4",
            "10.10.10.11",
            "22.22.23.24",
            "100.200.123.12"
        ),
        results
    );
    closer.close();
  }
}
