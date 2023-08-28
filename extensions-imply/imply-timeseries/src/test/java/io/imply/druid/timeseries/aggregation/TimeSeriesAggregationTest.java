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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.Util;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.imply.druid.timeseries.SimpleTimeSeriesBaseTest.MAX_ENTRIES;
import static org.apache.druid.query.QueryRunnerTestHelper.FULL_ON_INTERVAL;

public class TimeSeriesAggregationTest extends InitializedNullHandlingTest
{
  public static final DateTime DAY1 = DateTimes.of("2014-10-20T00:00:00.000Z");
  private static AggregationTestHelper timeseriesHelper;
  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static List<File> dirs;

  @BeforeClass
  public static void setup() throws Exception
  {
    TimeSeriesModule module = new TimeSeriesModule();
    SimpleModule granularityModule = new GranularityModule();
    List<Module> jacksonModules = new ArrayList<>(module.getJacksonModules());
    jacksonModules.add(granularityModule);
    timeseriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(jacksonModules, tempFolder);
    dirs = new ArrayList<>();

    final File outDir1 = tempFolder.newFolder("ts_agg_out1");
    timeseriesHelper.createIndex(
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
    timeseriesHelper.createIndex(
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
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .granularity(Granularities.ALL)
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            ImmutableList.of(
                SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                    "timeseries",
                    "dataPoints",
                    "__time",
                    null,
                    FULL_ON_INTERVAL,
                    MAX_ENTRIES
                ),
                DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                    "downsampledSumTimeseries",
                    "dataPoints",
                    "__time",
                    null,
                    7200000L,
                    FULL_ON_INTERVAL,
                    null
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
        FULL_ON_INTERVAL,
        null,
        null,
        219144,
        7200000L
    );
    
    Result<TimeseriesResultValue> expectedResult = new Result<>(
        DAY1,
        new TimeseriesResultValue(ImmutableMap.of(
            "timeseries", SimpleTimeSeriesContainer.createFromInstance(expectedTimeSeries),
            "downsampledSumTimeseries", SimpleTimeSeriesContainer.createFromInstance(expectedDownsampledSumTimeSeries)
        ))
    );
    TestHelper.assertExpectedResults(ImmutableList.of(expectedResult), results);
  }

  @Test
  public void testTimeseriesWithInterpolation()
  {
    Interval window = Intervals.of("2014-10-20T00:00:00.000Z/2014-10-20T09:03:00.000Z");
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .granularity(Granularities.ALL)
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            ImmutableList.of(
                SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                    "timeseries",
                    "dataPoints",
                    "__time",
                    null,
                    window,
                    2 * MAX_ENTRIES
                ),
                DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                    "downsampledSumTimeseries",
                    "dataPoints",
                    "__time",
                    null,
                    1800000L,
                    window,
                    null
                )
            )
        )
        .postAggregators(
            new ExpressionPostAggregator(
                "timeseries-pa",
                "timeseries_to_json(\"timeseries\")",
                null,
                Util.makeTimeSeriesMacroTable()
            ),
            new ExpressionPostAggregator(
                "timeseries-interpolation",
                "linear_interpolation(\"timeseries\", 'PT30M')",
                null,
                Util.makeTimeSeriesMacroTable()
            ),
            new ExpressionPostAggregator(
                "downsampledSumTimeseries-interpolation",
                "padding_interpolation(\"downsampledSumTimeseries\", 'PT30M')",
                null,
                Util.makeTimeSeriesMacroTable()
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
        window,
        2 * MAX_ENTRIES
    );

    double[] expectedDownsampledSumDataPoints = new double[]{0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0};
    SimpleTimeSeries expectedDownsampledSumTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDownsampledSumDataPoints),
        window,
        null,
        null,
        19,
        1800000L
    );

    long[] expectedInterpolatedTimestamps = new long[]{
        1413763200000L, 1413765000000L, 1413766800000L, 1413768600000L, 1413770400000L,
        1413772200000L, 1413774000000L, 1413775800000L, 1413777600000L, 1413779400000L,
        1413781200000L, 1413783000000L, 1413784800000L, 1413786600000L, 1413788400000L,
        1413790200000L, 1413792000000L, 1413793800000L, 1413795600000L
    };
    double[] expectedInterpolatedDataPoints = new double[]{
        0.0, 2.5, 5.0, 7.5, 10.0, 12.5, 15.0, 17.5, 20.0, 22.5,
        25.0, 27.5, 30.0, 32.5, 35.0, 37.5, 40.0, 42.5, 45.0
    };
    SimpleTimeSeries expectedInterpolatedTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedInterpolatedTimestamps),
        new ImplyDoubleArrayList(expectedInterpolatedDataPoints),
        window,
        2 * MAX_ENTRIES
    );

    double[] expectedInterpolatedDownsampledSumDataPoints = new double[]{
        0.0, 0.0, 5.0, 5.0, 10.0, 10.0, 15.0, 15.0, 20.0, 20.0,
        25.0, 25.0, 30.0, 30.0, 35.0, 35.0, 40.0, 40.0, 45.0
    };
    SimpleTimeSeries expectedInterpolatedDownsampledSumTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedInterpolatedTimestamps),
        new ImplyDoubleArrayList(expectedInterpolatedDownsampledSumDataPoints),
        window,
        null,
        null,
        19,
        1L
    );

    Result<TimeseriesResultValue> expectedResult = new Result<>(
        DAY1,
        new TimeseriesResultValue(
            ImmutableMap.<String, Object>builder()
                        .put("timeseries", SimpleTimeSeriesContainer.createFromInstance(expectedTimeSeries))
                        .put(
                            "timeseries-interpolation",
                            SimpleTimeSeriesContainer.createFromInstance(expectedInterpolatedTimeSeries)
                        )
                        .put("timeseries-pa", expectedTimeSeries)
                        .put("downsampledSumTimeseries", SimpleTimeSeriesContainer.createFromInstance(expectedDownsampledSumTimeSeries))
                        .put(
                            "downsampledSumTimeseries-interpolation",
                            SimpleTimeSeriesContainer.createFromInstance(expectedInterpolatedDownsampledSumTimeSeries)
                        )
                        .build()
            )
    );
    TestHelper.assertExpectedResults(ImmutableList.of(expectedResult), results);
  }

  @Test
  public void testBufferedAndHeapSumAggregator_emptyAggregation()
  {
    ByteBuffer buffer = ByteBuffer.allocate(1_000_000);
    SimpleTimeSeriesBuildAggregator simpleTimeSeriesBuildAggregator = new SimpleTimeSeriesBuildAggregator(
        null,
        null,
        Intervals.ETERNITY,
        100
    );
    Assert.assertNull(((SimpleTimeSeriesContainer) simpleTimeSeriesBuildAggregator.get()).computeSimple());

    SimpleTimeSeriesMergeAggregator simpleTimeSeriesMergeAggregator = new SimpleTimeSeriesMergeAggregator(
        null,
        Intervals.ETERNITY,
        100
    );
    Assert.assertNull(((SimpleTimeSeriesContainer) simpleTimeSeriesMergeAggregator.get()).computeSimple());

    SimpleTimeSeriesBuildBufferAggregator simpleTimeSeriesBuildBufferAggregator = new SimpleTimeSeriesBuildBufferAggregator(
        null,
        null,
        Intervals.ETERNITY,
        100
    );
    simpleTimeSeriesBuildBufferAggregator.init(buffer, 0);
    Assert.assertNull(((SimpleTimeSeriesContainer) simpleTimeSeriesBuildBufferAggregator.get(buffer, 0)).computeSimple());

    SimpleTimeSeriesMergeBufferAggregator simpleTimeSeriesMergeBufferAggregator = new SimpleTimeSeriesMergeBufferAggregator(
        null,
        Intervals.ETERNITY,
        100
    );
    simpleTimeSeriesMergeBufferAggregator.init(buffer, 0);
    Assert.assertNull(((SimpleTimeSeriesContainer) simpleTimeSeriesBuildBufferAggregator.get(buffer, 0)).computeSimple());
  }

  private static File fromCp(String filename)
  {
    return new File(Thread.currentThread().getContextClassLoader().getResource(filename).getFile());
  }
}
