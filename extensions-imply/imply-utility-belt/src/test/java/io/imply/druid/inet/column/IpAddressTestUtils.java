/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.license.ImplyLicenseManager;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.roaringbitmap.IntIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Maybe push some of this stuff into {@link AggregationTestHelper} someday
 */
public class IpAddressTestUtils
{
  public static final BitmapSerdeFactory roaringFactory = new RoaringBitmapSerdeFactory(null);
  public static final ObjectMapper JSON_MAPPER;
  public static final IpAddressModule LICENSED_IP_ADDRESS_MODULE;

  static {
    LICENSED_IP_ADDRESS_MODULE = new IpAddressModule();
    ImplyLicenseManager manager = Mockito.mock(ImplyLicenseManager.class);
    JSON_MAPPER = TestHelper.makeJsonMapper();
    JSON_MAPPER.registerModules(LICENSED_IP_ADDRESS_MODULE.getJacksonModules());
  }

  public static List<Segment> createIpAddressSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        "simple_ip_test_data.tsv",
        "simple_ip_test_data_record_parser.json",
        "simple_ip_test_data_aggregators.json",
        granularity,
        rollup,
        maxRowCount
    );
  }

  public static Segment createIpAddressIncrementalIndex(
      Granularity granularity,
      boolean rollup,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
      throws Exception
  {
    return createIncrementalIndex(
        "simple_ip_test_data.tsv",
        "simple_ip_test_data_record_parser.json",
        "simple_ip_test_data_aggregators.json",
        granularity,
        rollup,
        deserializeComplexMetrics,
        maxRowCount
    );
  }

  public static List<Segment> createIpPrefixSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        "simple_ip_prefix_test_data.tsv",
        "simple_ip_prefix_test_data_record_parser.json",
        "simple_ip_test_data_aggregators.json",
        granularity,
        rollup,
        maxRowCount
    );
  }

  public static Segment createIpPrefixIncrementalIndex(
      Granularity granularity,
      boolean rollup,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
      throws Exception
  {
    return createIncrementalIndex(
        "simple_ip_prefix_test_data.tsv",
        "simple_ip_prefix_test_data_record_parser.json",
        "simple_ip_test_data_aggregators.json",
        granularity,
        rollup,
        deserializeComplexMetrics,
        maxRowCount
    );
  }

  public static List<Segment> createSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      String inputFileName,
      String parserJsonFileName,
      String aggJsonFileName,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    File inputFile = readFileFromClasspath(inputFileName);
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString(parserJsonFileName);
    String aggJson = readFileFromClasspathAsString(aggJsonFileName);

    helper.createIndex(
        inputDataStream,
        parserJson,
        aggJson,
        segmentDir,
        0,
        granularity,
        maxRowCount,
        rollup
    );

    final List<Segment> segments = Lists.transform(
        ImmutableList.of(segmentDir),
        dir -> {
          try {
            return new QueryableIndexSegment(helper.getIndexIO().loadIndex(dir), SegmentId.dummy(""));
          }
          catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
    );

    return segments;
  }

  public static Segment createIncrementalIndex(
      String inputFileName,
      String parserJsonFileName,
      String aggJsonFileName,
      Granularity granularity,
      boolean rollup,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
      throws Exception
  {
    File inputFile = readFileFromClasspath(inputFileName);
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString(parserJsonFileName);
    String aggJson = readFileFromClasspathAsString(aggJsonFileName);
    StringInputRowParser parser = JSON_MAPPER.readValue(parserJson, StringInputRowParser.class);

    LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
    List<AggregatorFactory> aggregatorSpecs = JSON_MAPPER.readValue(
        aggJson,
        new TypeReference<List<AggregatorFactory>>()
        {
        }
    );
    IncrementalIndex index = AggregationTestHelper.createIncrementalIndex(
        iter,
        parser,
        parser.getParseSpec().getDimensionsSpec().getDimensions(),
        aggregatorSpecs.toArray(new AggregatorFactory[0]),
        0,
        granularity,
        deserializeComplexMetrics,
        maxRowCount,
        rollup
    );
    return new IncrementalIndexSegment(index, SegmentId.dummy("test_datasource"));
  }

  public static Segment createIpAddressDefaultHourlyIncrementalIndex() throws Exception
  {
    return createIpAddressIncrementalIndex(Granularities.HOUR, true, true, 1000);
  }

  public static Segment createIpAddressDefaultDailyIncrementalIndex() throws Exception
  {
    return createIpAddressIncrementalIndex(Granularities.DAY, true, true, 1000);
  }

  public static Segment createIpPrefixDefaultHourlyIncrementalIndex() throws Exception
  {
    return createIpPrefixIncrementalIndex(Granularities.HOUR, true, true, 1000);
  }

  public static Segment createIpPrefixDefaultDailyIncrementalIndex() throws Exception
  {
    return createIpPrefixIncrementalIndex(Granularities.DAY, true, true, 1000);
  }

  public static List<Segment> createIpAddressDefaultHourlySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createIpAddressSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        1000
    );
  }

  public static List<Segment> createIpAddressDefaultDaySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createIpAddressSegments(
        helper,
        tempFolder,
        Granularities.DAY,
        true,
        1000
    );
  }

  public static List<Segment> createIpPrefixDefaultHourlySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createIpPrefixSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        1000
    );
  }

  public static List<Segment> createIpPrefixDefaultDaySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createIpPrefixSegments(
        helper,
        tempFolder,
        Granularities.DAY,
        true,
        1000
    );
  }

  public static File readFileFromClasspath(String fileName)
  {
    return new File(IpAddressIngestionTest.class.getClassLoader().getResource(fileName).getFile());
  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(readFileFromClasspath(fileName), StandardCharsets.UTF_8).read();
  }

  public static void verifyResults(RowSignature rowSignature, List<ResultRow> results, List<Object[]> expected)
  {
    Assert.assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      final Object[] resultRow = results.get(i).getArray();
      Assert.assertEquals(expected.get(i).length, resultRow.length);
      for (int j = 0; j < resultRow.length; j++) {
        if (rowSignature.getColumnType(j).map(t -> t.anyOf(ValueType.DOUBLE, ValueType.FLOAT)).orElse(false)) {
          Assert.assertEquals((Double) resultRow[j], (Double) expected.get(i)[j], 0.01);
        } else {
          Assert.assertEquals(resultRow[j], expected.get(i)[j]);
        }
      }
    }
  }

  public static DictionaryEncodedIpAddressBlobColumnIndexSupplier makeIpAddressIndexSupplier() throws IOException
  {
    ByteBuffer dictionaryBuffer = ByteBuffer.allocate(1 << 12);
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    writeIpAddressDictionary(dictionaryBuffer);
    dictionaryBuffer.position(0);
    writeBitmap(bitmapsBuffer);
    bitmapsBuffer.position(0);

    GenericIndexed<ByteBuffer> dictionary = GenericIndexed.read(
        dictionaryBuffer,
        IpAddressComplexTypeSerde.NULLABLE_BYTE_BUFFER_STRATEGY
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(
        bitmapsBuffer,
        roaringFactory.getObjectStrategy()
    );

    return new DictionaryEncodedIpAddressBlobColumnIndexSupplier(
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary
    );
  }


  public static DictionaryEncodedIpAddressBlobColumnIndexSupplier makeIpPrefixIndexSupplier() throws IOException
  {
    ByteBuffer dictionaryBuffer = ByteBuffer.allocate(1 << 12);
    ByteBuffer bitmapsBuffer = ByteBuffer.allocate(1 << 12);

    writeIpPrefixDictionary(dictionaryBuffer);
    dictionaryBuffer.position(0);
    writeBitmap(bitmapsBuffer);
    bitmapsBuffer.position(0);

    GenericIndexed<ByteBuffer> dictionary = GenericIndexed.read(
        dictionaryBuffer,
        IpPrefixComplexTypeSerde.NULLABLE_BYTE_BUFFER_STRATEGY
    );

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(
        bitmapsBuffer,
        roaringFactory.getObjectStrategy()
    );

    return new DictionaryEncodedIpAddressBlobColumnIndexSupplier(
        roaringFactory.getBitmapFactory(),
        bitmaps,
        dictionary
    );
  }

  public static void writeIpPrefixDictionary(ByteBuffer buffer) throws IOException
  {
    GenericIndexedWriter<IpPrefixBlob> dictionaryWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "ip",
        IpPrefixBlob.STRATEGY
    );
    dictionaryWriter.open();
    dictionaryWriter.write(IpPrefixBlob.ofString("1.2.3.4/16"));
    dictionaryWriter.write(IpPrefixBlob.ofString("10.10.10.10/8"));
    dictionaryWriter.write(IpPrefixBlob.ofString("1:2:3:0:0:6::/64"));
    dictionaryWriter.write(IpPrefixBlob.ofString("4:5:6:7:0:aa::/128"));
    writeToBuffer(buffer, dictionaryWriter);
  }

  public static void writeIpAddressDictionary(ByteBuffer buffer) throws IOException
  {
    GenericIndexedWriter<IpAddressBlob> dictionaryWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "ip",
        IpAddressBlob.STRATEGY
    );
    dictionaryWriter.open();
    dictionaryWriter.write(IpAddressBlob.ofString("1.2.3.4"));
    dictionaryWriter.write(IpAddressBlob.ofString("10.10.10.10"));
    dictionaryWriter.write(IpAddressBlob.ofString("1:2:3:0:0:6::"));
    dictionaryWriter.write(IpAddressBlob.ofString("4:5:6:7:0:aa::"));
    writeToBuffer(buffer, dictionaryWriter);
  }

  public static void writeBitmap(ByteBuffer buffer) throws IOException
  {
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();
    bitmapWriter.write(fillBitmap(1, 3, 7, 8));
    bitmapWriter.write(fillBitmap(0, 9));
    bitmapWriter.write(fillBitmap(2, 5));
    bitmapWriter.write(fillBitmap(4, 6));
    writeToBuffer(buffer, bitmapWriter);
  }

  public static ImmutableBitmap fillBitmap(int... rows)
  {
    MutableBitmap bitmap = roaringFactory.getBitmapFactory().makeEmptyMutableBitmap();
    for (int i : rows) {
      bitmap.add(i);
    }
    return roaringFactory.getBitmapFactory().makeImmutableBitmap(bitmap);
  }


  public static void checkBitmap(ImmutableBitmap bitmap, int... expectedRows)
  {
    IntIterator iterator = bitmap.iterator();
    for (int i : expectedRows) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(i, iterator.next());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  public static void writeToBuffer(ByteBuffer buffer, Serializer serializer) throws IOException
  {
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };

    serializer.writeTo(channel, null);
  }
}
