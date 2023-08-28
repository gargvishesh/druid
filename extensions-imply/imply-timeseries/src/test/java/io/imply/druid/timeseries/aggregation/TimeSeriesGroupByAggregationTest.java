/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.io.Files;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory.DEFAULT_MAX_ENTRIES;

public class TimeSeriesGroupByAggregationTest
{
  private static AggregationTestHelper groupbyHelper;
  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static List<File> dirs;

  @BeforeClass
  public static void setup() throws Exception
  {
    TimeSeriesModule module = new TimeSeriesModule();
    TimeSeriesModule.registerSerde();
    SimpleModule granularityModule = new GranularityModule();
    List<Module> jacksonModules = new ArrayList<>(module.getJacksonModules());
    jacksonModules.add(granularityModule);
    groupbyHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        jacksonModules,
        new GroupByQueryConfig(),
        tempFolder
    );
    dirs = new ArrayList<>();

    final File outDir1 = tempFolder.newFolder("ts_agg_out1");
    groupbyHelper.createIndex(
        fromCp("simple_test_data.tsv"),
        Files.asCharSource(fromCp("simple_test_data_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"zeroCol\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir1,
        0,
        Granularities.NONE,
        100
    );
    dirs.add(outDir1);

    final File outDir2 = tempFolder.newFolder("ts_agg_out2");
    groupbyHelper.createIndex(
        fromCp("simple_test_data_2.tsv"),
        Files.asCharSource(fromCp("simple_test_data_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"zeroCol\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir2,
        0,
        Granularities.NONE,
        100
    );
    dirs.add(outDir2);
  }

  @Test
  public void testGroupBy()
  {
    String query = StringUtils.format(
        "{"
        + "  \"queryType\": \"groupBy\","
        + "  \"dataSource\": \"%s\","
        + "  \"granularity\": \"ALL\","
        + "  \"dimensions\": [],"
        + "  \"aggregations\": ["
        + "  {\"type\": \"timeseries\", \"name\": \"timeseries\", \"timeColumn\": \"__time\", \"dataColumn\" : \"dataPoints\","
        + " \"window\" : \"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"},"
        + "  {\"type\": \"downsampledSumTimeseries\", \"name\": \"downsampledSumTS\", \"timeColumn\": \"__time\", \"dataColumn\" : \"dataPoints\","
        + " \"timeBucketMillis\" : 7200000, \"window\" : \"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"},"
        + "  {\"type\": \"sumTimeseries\", \"name\": \"sumTS\", \"timeseriesColumn\": \"fuu\", "
        + " \"window\" : \"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
        + "  ],"
        + "  \"intervals\": [\"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"]"
        + "  }",
        QueryRunnerTestHelper.DATA_SOURCE
    );
    Sequence<Object> results = groupbyHelper.runQueryOnSegments(dirs, query);
    ResultRow resultRow = (ResultRow) results.toList().get(0);
    long[] expectedTimestamps = new long[]{
        1413763200000L, 1413766800000L, 1413770400000L, 1413774000000L, 1413777600000L,
        1413781200000L, 1413784800000L, 1413788400000L, 1413792000000L, 1413795600000L
    };
    double[] expectedDataPoints = new double[]{0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0};
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDataPoints),
        Intervals.of("2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z"),
        DEFAULT_MAX_ENTRIES
    );

    long[] expectedDownsampledSumTimestamps = new long[]{
        1413763200000L,
        1413770400000L,
        1413777600000L,
        1413784800000L,
        1413792000000L
    };
    double[] expectedDownsampledSumDataPoints = new double[]{5, 25, 45, 65, 85};
    SimpleTimeSeries expectedDownsampledSumTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedDownsampledSumTimestamps),
        new ImplyDoubleArrayList(expectedDownsampledSumDataPoints),
        Intervals.of("2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z"),
        null,
        null,
        30684,
        7200000L
    );

    long[] expectedSumTimestamps = new long[]{0L};
    double[] expectedSumDataPoints = new double[]{225D};
    SimpleTimeSeries expectedSumTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedSumTimestamps),
        new ImplyDoubleArrayList(expectedSumDataPoints),
        Intervals.ETERNITY,
        DEFAULT_MAX_ENTRIES
    );
    Assert.assertEquals(SimpleTimeSeriesContainer.createFromInstance(expectedTimeSeries), resultRow.get(0));
    Assert.assertEquals(SimpleTimeSeriesContainer.createFromInstance(expectedDownsampledSumTimeSeries), resultRow.get(1));
    Assert.assertEquals(SimpleTimeSeriesContainer.createFromInstance(expectedSumTimeSeries), resultRow.get(2));
  }


  private static File fromCp(String filename)
  {
    return new File(Thread.currentThread().getContextClassLoader().getResource(filename).getFile());
  }
}
