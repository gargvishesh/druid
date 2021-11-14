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
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.timeseries.SimpleTimeSeries;
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
    module.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    SimpleModule granularityModule = new GranularityModule();
    List<Module> jacksonModules = new ArrayList<>(module.getJacksonModules());
    jacksonModules.add(granularityModule);
    groupbyHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(jacksonModules, new GroupByQueryConfig(), tempFolder);
    dirs = new ArrayList<>();

    final File outDir1 = tempFolder.newFolder("ts_agg_out1");
    groupbyHelper.createIndex(
        fromCp("simple_test_data.tsv"),
        Files.asCharSource(fromCp("simple_test_data_record_parser.json"), StandardCharsets.UTF_8).read(),
        "[]",
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
        "[]",
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
    String query = StringUtils.format("{"
        + "  \"queryType\": \"groupBy\","
        + "  \"dataSource\": \"%s\","
        + "  \"granularity\": \"ALL\","
        + "  \"dimensions\": [],"
        + "  \"aggregations\": ["
        + "  {\"type\": \"timeseries\", \"name\": \"timeseries\", \"timeColumn\": \"__time\", \"dataColumn\" : \"dataPoints\","
                                      + " \"window\" : \"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"},"
        + "  {\"type\": \"avgTimeseries\", \"name\": \"avgTS\", \"timeColumn\": \"__time\", \"dataColumn\" : \"dataPoints\","
                                      + " \"timeBucketMillis\" : 7200000, \"window\" : \"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"},"
        + "  {\"type\": \"deltaTimeseries\", \"name\": \"deltaTS\", \"timeColumn\": \"__time\", \"dataColumn\" : \"dataPoints\","
                                      + " \"timeBucketMillis\" : 10800000, \"window\" : \"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"}"
        + "  ],"
        + "  \"intervals\": [\"2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z\"]"
        + "  }",
        QueryRunnerTestHelper.DATA_SOURCE);
    Sequence<Object> results = groupbyHelper.runQueryOnSegments(dirs, query);
    ResultRow resultRow = (ResultRow) results.toList().get(0);
    long[] expectedTimestamps = new long[]{1413763200000L, 1413766800000L, 1413770400000L, 1413774000000L, 1413777600000L,
                                           1413781200000L, 1413784800000L, 1413788400000L, 1413792000000L, 1413795600000L};
    double[] expectedDataPoints = new double[]{0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0};
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedTimestamps),
                                                               new ImplyDoubleArrayList(expectedDataPoints),
                                                               Intervals.of("2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z"),
                                                               DEFAULT_MAX_ENTRIES);

    long[] expectedMeanTimestamps = new long[]{1413763200000L, 1413770400000L, 1413777600000L, 1413784800000L, 1413792000000L};
    double[] expectedMeanDataPoints = new double[]{2.5, 12.5, 22.5, 32.5, 42.5};
    SimpleTimeSeries expectedAvgTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedMeanTimestamps),
                                                                  new ImplyDoubleArrayList(expectedMeanDataPoints),
                                                                  Intervals.of("2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z"),
                                                                  30684);

    long[] expectedDeltaTimestamps = new long[]{1413763200000L, 1413774000000L, 1413784800000L, 1413795600000L};
    double[] expectedDeltaDataPoints = new double[]{10, 10.0, 10.0, 0.0};
    SimpleTimeSeries expectedDeltaTimeSeries = new SimpleTimeSeries(new ImplyLongArrayList(expectedDeltaTimestamps),
                                                                    new ImplyDoubleArrayList(expectedDeltaDataPoints),
                                                                    Intervals.of("2014-10-20T00:00:00.000Z/2021-10-20T00:00:00.000Z"),
                                                                    20456);
    Assert.assertEquals(expectedTimeSeries, resultRow.get(0));
    Assert.assertEquals(expectedAvgTimeSeries, resultRow.get(1));
    Assert.assertEquals(expectedDeltaTimeSeries, resultRow.get(2));
  }


  private static File fromCp(String filename)
  {
    return new File(Thread.currentThread().getContextClassLoader().getResource(filename).getFile());
  }
}
