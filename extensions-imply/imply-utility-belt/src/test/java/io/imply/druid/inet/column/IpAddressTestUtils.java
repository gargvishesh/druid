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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Maybe push some of this stuff into {@link AggregationTestHelper} someday
 */
public class IpAddressTestUtils
{
  public static final ObjectMapper JSON_MAPPER;

  static  {
    JSON_MAPPER = TestHelper.makeJsonMapper();
    JSON_MAPPER.registerModules(new IpAddressModule().getJacksonModules());
  }


  public static List<Segment> createSegments(
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

  public static Segment createIncrementalIndex(
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
        aggregatorSpecs.toArray(new AggregatorFactory[0]),
        0,
        granularity,
        deserializeComplexMetrics,
        maxRowCount,
        rollup
    );
    return new IncrementalIndexSegment(index, SegmentId.dummy("test_datasource"));
  }

  public static Segment createDefaultHourlyIncrementalIndex() throws Exception
  {
    return createIncrementalIndex(Granularities.HOUR, true, true, 1000);
  }

  public static Segment createDefaultDailyIncrementalIndex() throws Exception
  {
    return createIncrementalIndex(Granularities.DAY, true, true, 1000);
  }

  public static List<Segment> createDefaultHourlySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        Granularities.HOUR,
        true,
        1000
    );
  }

  public static List<Segment> createDefaultDaySegments(AggregationTestHelper helper, TemporaryFolder tempFolder)
      throws Exception
  {
    return createSegments(
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
}
