/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 *  of Imply Data, Inc.
 */

package io.imply.druid.currency;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
@SuppressWarnings({"unchecked"})
public class CurrencySumQueryTest extends InitializedNullHandlingTest
{
  private static final String VISITOR_ID = "visitor_id";
  private static final String REVENUE = "revenue";
  private final IndexBuilder indexBuilder;
  private final Function<IndexBuilder, Pair<Segment, Closeable>> finisher;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Segment segment = null;
  private Closeable closeable = null;

  public CurrencySumQueryTest(
      String testName,
      Function<IndexBuilder, Pair<Segment, Closeable>> finisher
  )
  {
    this.indexBuilder = IndexBuilder
        .create()
        .schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(
                    new DimensionsSpec(
                        DimensionsSpec.getDefaultSchemas(ImmutableList.of(VISITOR_ID)),
                        null,
                        null
                    )
                )
                .withMetrics(
                    new CountAggregatorFactory("count"),
                    new DoubleSumAggregatorFactory(REVENUE, REVENUE)
                )
                .withQueryGranularity(Granularities.NONE)
                .build()
        ).rows(
            ImmutableList.of(
                new MapBasedInputRow(
                    CurrencySumSerdeTest.DAY1.getMillis(),
                    Lists.newArrayList(VISITOR_ID),
                    ImmutableMap.of(VISITOR_ID, "0", REVENUE, 10)
                ),
                new MapBasedInputRow(
                    CurrencySumSerdeTest.DAY1.getMillis() + 10,
                    Lists.newArrayList(VISITOR_ID),
                    ImmutableMap.of(VISITOR_ID, "1", REVENUE, 100)
                ),
                new MapBasedInputRow(
                    CurrencySumSerdeTest.DAY2.getMillis(),
                    Lists.newArrayList(VISITOR_ID),
                    ImmutableMap.of(VISITOR_ID, "2", REVENUE, 1000)
                ),
                new MapBasedInputRow(
                    CurrencySumSerdeTest.DAY2.getMillis(),
                    Lists.newArrayList(VISITOR_ID),
                    ImmutableMap.of(VISITOR_ID, "3", REVENUE, 1500)
                ),
                new MapBasedInputRow(
                    CurrencySumSerdeTest.DAY3.getMillis(),
                    Lists.newArrayList(VISITOR_ID),
                    ImmutableMap.of(VISITOR_ID, "4", REVENUE, 10000)
                )
            )
        );
    this.finisher = finisher;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Map<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finishers = ImmutableMap.of(
        "incremental", input -> {
          final IncrementalIndex index = input.buildIncrementalIndex();
          return Pair.of(new IncrementalIndexSegment(index, SegmentId.dummy("dummy")), index);
        },
        "mmapped", input -> {
          final QueryableIndex index = input.buildMMappedIndex();
          return Pair.of(new QueryableIndexSegment(index, SegmentId.dummy("dummy")), index);
        },
        "mmappedMerged", input -> {
          final QueryableIndex index = input.buildMMappedMergedIndex();
          return Pair.of(new QueryableIndexSegment(index, SegmentId.dummy("dummy")), index);
        }
    );

    for (final Map.Entry<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finisherEntry : finishers.entrySet()) {
      final String testName = StringUtils.format("finisher[%s]", finisherEntry.getKey());
      constructors.add(new Object[]{testName, finisherEntry.getValue()});
    }

    return constructors;
  }

  @Before
  public void setUp() throws Exception
  {
    indexBuilder.tmpDir(temporaryFolder.newFolder());
    final Pair<Segment, Closeable> pair = finisher.apply(indexBuilder);
    segment = pair.lhs;
    closeable = pair.rhs;
  }

  @After
  public void tearDown() throws Exception
  {
    closeable.close();
  }

  @Test
  public void testTimeseries()
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .granularity(Granularities.ALL)
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory("count", "count"),
                                          new CurrencySumAggregatorFactory(
                                              "revenue_converted",
                                              REVENUE,
                                              CurrencySumSerdeTest.CONVERSIONS
                                          )
                                      )
                                  )
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results =
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(QueryPlus.wrap(query), DefaultResponseContext.createEmpty())
         .toList();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            CurrencySumSerdeTest.DAY1,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 5L)
                    .put("count", 5L)
                    .put("revenue_converted", 47720.0)
                    .build()
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter()
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .granularity(Granularities.ALL)
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .filters(
                                      new InDimFilter(VISITOR_ID, Arrays.asList("2", "3", "4"), null)
                                  )
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory("count", "count"),
                                          new CurrencySumAggregatorFactory(
                                              "revenue_converted",
                                              REVENUE,
                                              CurrencySumSerdeTest.CONVERSIONS
                                          )
                                      )
                                  )
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results =
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(QueryPlus.wrap(query), DefaultResponseContext.createEmpty())
         .toList();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            CurrencySumSerdeTest.DAY1,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 3L)
                    .put("count", 3L)
                    .put("revenue_converted", 47500.0)
                    .build()
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTopN()
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        new StupidPool<>("CurrencyConversionQueryTest-bufferPool", () -> ByteBuffer.allocate(10485760)),
        new TopNQueryQueryToolChest(new TopNQueryConfig()),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(VISITOR_ID)
        .metric("revenue_converted")
        .threshold(5)
        .aggregators(
            Lists.newArrayList(
                new LongSumAggregatorFactory("count", "count"),
                new CurrencySumAggregatorFactory("revenue_converted", REVENUE, CurrencySumSerdeTest.CONVERSIONS)
            )
        )
        .build();

    Iterable<Result<TopNResultValue>> results =
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(QueryPlus.wrap(query), DefaultResponseContext.createEmpty())
         .toList();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            CurrencySumSerdeTest.DAY1,
            new TopNResultValue(
                ImmutableList.of(
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "4",
                        "revenue_converted", 40000.0,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "3",
                        "revenue_converted", 4500.0,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "2",
                        "revenue_converted", 3000.0,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "1",
                        "revenue_converted", 200.0,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        VISITOR_ID, "0",
                        "revenue_converted", 20.0,
                        "count", 1L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testGroupBy() throws IOException
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    final Pair<GroupByQueryRunnerFactory, Closer> factoryPair = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
    final GroupByQueryRunnerFactory factory = factoryPair.lhs;
    final Closer closer = factoryPair.rhs;

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                     .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
                                     .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL)
                                     .setDimensions(ImmutableList.of())
                                     .setAggregatorSpecs(
                                         Lists.newArrayList(
                                             new LongSumAggregatorFactory("count", "count"),
                                             new CurrencySumAggregatorFactory(
                                                 "revenue_converted",
                                                 REVENUE,
                                                 CurrencySumSerdeTest.CONVERSIONS
                                             )
                                         )
                                     )
                                     .build();

    List<ResultRow> results =
        new FinalizeResultsQueryRunner(
            factory.getToolchest().mergeResults(
                factory.mergeRunners(
                    Execs.directExecutor(),
                    ImmutableList.of(factory.createRunner(segment))
                )
            ),
            factory.getToolchest()
        ).run(QueryPlus.wrap(query), DefaultResponseContext.createEmpty())
         .toList();

    List<ResultRow> expectedResults = Collections.singletonList(ResultRow.of(5L, 47720.0));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    closer.close();
  }
}
