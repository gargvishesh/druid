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
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.Util;
import io.imply.druid.timeseries.expression.MaxOverTimeseriesExprMacro;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SumTimeSeriesAggregationTest extends InitializedNullHandlingTest
{
  public static final DateTime DAY1 = DateTimes.of("1970-01-01T00:00:00.000Z");
  private static AggregationTestHelper timeseriesHelper;
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
    timeseriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(jacksonModules, tempFolder);
    dirs = new ArrayList<>();

    final File outDir1 = tempFolder.newFolder("ts_agg_out1");
    timeseriesHelper.createIndex(
        fromCp("simple_test_data_2.tsv"),
        Files.asCharSource(fromCp("simple_time_series_serde_test_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"zeroCol\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir1,
        0,
        Granularities.ALL,
        100
    );
    dirs.add(outDir1);

    final File outDir2 = tempFolder.newFolder("ts_agg_out2");
    timeseriesHelper.createIndex(
        fromCp("simple_test_data.tsv"),
        Files.asCharSource(fromCp("simple_time_series_serde_test_record_parser.json"), StandardCharsets.UTF_8).read(),
        "["
        + "{\"type\": \"timeseries\", "
        + "\"name\": \"fuu\", "
        + "\"timeColumn\": \"zeroCol\", "
        + "\"dataColumn\": \"dataPoints\", "
        + "\"postprocessing\": \"[]\", "
        + "\"maxEntries\": \"100\"}]",
        outDir2,
        0,
        Granularities.ALL,
        100
    );
    dirs.add(outDir2);
  }

  @Test
  public void testSumTimeSeries()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .granularity(Granularities.ALL)
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .virtualColumns(
            new ExpressionVirtualColumn(
                "max_val_ts",
                StringUtils.format("%s(fuu)", MaxOverTimeseriesExprMacro.NAME),
                ColumnType.DOUBLE,
                Util.makeTimeSeriesMacroTable()
            )
        )
        .aggregators(
            ImmutableList.of(
                SumTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                    "sumtimeseries",
                    "fuu",
                    null
                ),
                new DoubleMaxAggregatorFactory("m1", "max_val_ts")
            )
        )
        .build();

    Iterable<Result<TimeseriesResultValue>> results =
        timeseriesHelper.runQueryOnSegments(dirs, query).toList();

    long[] expectedTimestamps = new long[]{0L};
    double[] expectedDataPoints = new double[]{225.0};
    SimpleTimeSeries expectedTimeSeries = new SimpleTimeSeries(
        new ImplyLongArrayList(expectedTimestamps),
        new ImplyDoubleArrayList(expectedDataPoints),
        Intervals.ETERNITY,
        SimpleTimeSeriesAggregatorFactory.DEFAULT_MAX_ENTRIES
    );

    Result<TimeseriesResultValue> expectedResult = new Result<>(
        DAY1,
        new TimeseriesResultValue(ImmutableMap.of(
            "sumtimeseries", SimpleTimeSeriesContainer.createFromInstance(expectedTimeSeries),
            "m1", 45D
        ))
    );
    TestHelper.assertExpectedResults(ImmutableList.of(expectedResult), results);
  }

  private static File fromCp(String filename)
  {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(filename);
    Preconditions.checkNotNull(resource, "can't find file %s", filename);
    return new File(resource.getFile());
  }
}
