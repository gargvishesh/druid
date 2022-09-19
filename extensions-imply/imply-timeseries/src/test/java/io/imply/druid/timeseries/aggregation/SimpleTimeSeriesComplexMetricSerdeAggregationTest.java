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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.interpolation.Interpolator;
import io.imply.druid.timeseries.postaggregators.InterpolationPostAggregator;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;
import static org.apache.druid.query.QueryRunnerTestHelper.FULL_ON_INTERVAL;

public class SimpleTimeSeriesComplexMetricSerdeAggregationTest extends InitializedNullHandlingTest
{
  public static final DateTime DAY1 = DateTimes.of("2014-10-20T00:00:00.000Z");
  private static AggregationTestHelper timeseriesHelper;
  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static List<File> dirs;
  private static AggregationTestHelper groupByQueryAggregationTestHelper;

  @BeforeClass
  public static void setup() throws Exception
  {
    TimeSeriesModule module = new TimeSeriesModule();
    TimeSeriesModule.registerSerde();
    module.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    SimpleModule granularityModule = new GranularityModule();
    List<Module> jacksonModules = new ArrayList<>(module.getJacksonModules());
    jacksonModules.add(granularityModule);
    timeseriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(jacksonModules, tempFolder);
    groupByQueryAggregationTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        jacksonModules,
        new GroupByQueryConfig(),
        tempFolder
    );
    dirs = new ArrayList<>();

    final File outDir1 = tempFolder.newFolder("ts_agg_out1");
    timeseriesHelper.createIndex(
        fromCp("simple_test_data.tsv"),
        Files.asCharSource(fromCp("simple_time_series_serde_test_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"__time\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir1,
        0,
        Granularities.DAY,
        100
    );
    dirs.add(outDir1);

    final File outDir2 = tempFolder.newFolder("ts_agg_out2");
    timeseriesHelper.createIndex(
        fromCp("simple_test_data_2.tsv"),
        Files.asCharSource(fromCp("simple_time_series_serde_test_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"__time\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir2,
        0,
        Granularities.DAY,
        100
    );
    dirs.add(outDir2);
  }

  @Test
  public void testSimpleTimeseriesSerdeReadAndAgg()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .granularity(Granularities.ALL)
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            ImmutableList.of(
                // TIMESERIES(__time, dataPoints) AS timeseries
                SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                    "timeseries-name",
                    null,
                    null,
                    "fuu",
                    null,
                    FULL_ON_INTERVAL,
                    MAX_ENTRIES
                )
            )
        )
        .build();

    Iterable<Result<TimeseriesResultValue>> results =
        timeseriesHelper.runQueryOnSegments(dirs, query).toList();

    long[] expectedTimestamps = new long[]{
        1413763200000L, 1413766800000L, 1413770400000L, 1413774000000L, 1413777600000L,
        1413781200000L, 1413784800000L, 1413788400000L, 1413792000000L, 1413795600000L
    };
    double[] expectedDataPoints = new double[]{0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0};
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDataPoints),
        FULL_ON_INTERVAL,
        MAX_ENTRIES
    );

    Result<TimeseriesResultValue> expectedResult = new Result<>(
        DAY1,
        new TimeseriesResultValue(ImmutableMap.of("timeseries-name", expectedTimeSeries))
    );
    TestHelper.assertExpectedResults(ImmutableList.of(expectedResult), results);
  }

  @Test
  public void testGroupByAndPostAggSimpleTimeseriesSerde()
  {
    Interval dataIntervalPlusOneHour = Intervals.of("2014-10-20T00/2014-10-20T10");
    GroupByQuery groupByQuery = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(FULL_ON_INTERVAL)),
        null,
        null,
        Granularities.ALL,
        Collections.emptyList(),
        ImmutableList.of(
            SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                "timeseries-name",
                null,
                null,
                "fuu",
                null,
                dataIntervalPlusOneHour,
                32
            )
        ),
        ImmutableList.of(
            new InterpolationPostAggregator(
                "baz",
                new FieldAccessPostAggregator("baz", "timeseries-name"),
                Interpolator.LINEAR,
                30 * 60 * 1000L
            )
        ),
        null,
        null,
        null,
        null
    );

    List<ResultRow> resultRows = groupByQueryAggregationTestHelper.runQueryOnSegments(dirs, groupByQuery).toList();

    Assert.assertEquals(1, resultRows.size());

    ResultRow resultRow = resultRows.get(0); // base agg
    SimpleTimeSeries result = (SimpleTimeSeries) resultRow.get(0);
    SimpleTimeSeries resultPostAgg = (SimpleTimeSeries) resultRow.get(1); //post-agg

    long[] expectedTimestamps = new long[]{
        1413763200000L, 1413766800000L, 1413770400000L, 1413774000000L, 1413777600000L,
        1413781200000L, 1413784800000L, 1413788400000L, 1413792000000L, 1413795600000L
    };
    double[] expectedDataPoints = new double[]{0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0};
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDataPoints),
        dataIntervalPlusOneHour,
        MAX_ENTRIES
    );
    long[] expectedTimestampsPostAgg = new long[]{
        1413763200000L, 1413765000000L, 1413766800000L, 1413768600000L, 1413770400000L, 1413772200000L, 1413774000000L,
        1413775800000L, 1413777600000L, 1413779400000L, 1413781200000L, 1413783000000L, 1413784800000L, 1413786600000L,
        1413788400000L, 1413790200000L, 1413792000000L, 1413793800000L, 1413795600000L, 1413797400000L

    };
    double[] expectedDataPointsPostAgg = new double[]{
        0.0, 2.5, 5.0, 7.5, 10.0, 12.5, 15.0, 17.5, 20.0, 22.5,
        25.0, 27.5, 30.0, 32.5, 35.0, 37.5, 40.0, 42.5, 45.0, 47.5
    };
    SimpleTimeSeries expectedTimeSeriesPostAgg = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestampsPostAgg),
        new ImplyDoubleArrayList(expectedDataPointsPostAgg),
        dataIntervalPlusOneHour,
        MAX_ENTRIES
    );

    Assert.assertNotNull(result);
    Assert.assertEquals(expectedTimeSeries.asSimpleTimeSeriesData(), result.asSimpleTimeSeriesData());

    Assert.assertNotNull(resultPostAgg);
    Assert.assertEquals(expectedTimeSeriesPostAgg.asSimpleTimeSeriesData(), resultPostAgg.asSimpleTimeSeriesData());
  }

  private static File fromCp(String filename)
  {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(filename);
    Preconditions.checkNotNull(resource, "can't find file %s", filename);
    return new File(resource.getFile());
  }
}
