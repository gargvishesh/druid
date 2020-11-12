/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.currency;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class CurrencySumBenchmark
{
  static {
    NullHandling.initializeForTests();
  }
  private static final int NUM_ROWS = 50000;

  QueryableIndexSegment segment;
  AggregatorFactory aggregatorFactory;
  TimeseriesQuery doubleSumQuery;
  TimeseriesQuery currencyQuery;
  TimeseriesQueryRunnerFactory factory;

  @Setup
  public void setup()
  {
    final DataGenerator generator = new DataGenerator(
        ImmutableList.of(
            GeneratorColumnSchema.makeContinuousUniform("usd", ValueType.FLOAT, true, 1, 0.0, 0, 1)
        ),
        42L,
        Intervals.of("2016-07-01/P1M"),
        NUM_ROWS
    );
    final IndexBuilder indexBuilder = IndexBuilder
        .create()
        .schema(
            new IncrementalIndexSchema.Builder().withMetrics(
                new AggregatorFactory[]{
                    new DoubleSumAggregatorFactory(
                        "usd",
                        "usd"
                    )
                }
            ).build()
        )
        .tmpDir(FileUtils.createTempDir());
    indexBuilder.rows(() -> IntStream.range(0, NUM_ROWS).mapToObj(i -> generator.nextRow()).iterator());
    segment = new QueryableIndexSegment(indexBuilder.buildMMappedIndex(), SegmentId.dummy("dummy"));
    aggregatorFactory = new CurrencySumAggregatorFactory(
        "eur",
        "usd",
        ImmutableMap.<DateTime, Double>builder()
            .put(DateTimes.of("2016-07-01T00:00:00.000Z"), 0.899531)
            .put(DateTimes.of("2016-07-02T00:00:00.000Z"), 0.89699)
            .put(DateTimes.of("2016-07-03T00:00:00.000Z"), 0.89699)
            .put(DateTimes.of("2016-07-04T00:00:00.000Z"), 0.897827)
            .put(DateTimes.of("2016-07-05T00:00:00.000Z"), 0.898707)
            .put(DateTimes.of("2016-07-06T00:00:00.000Z"), 0.903489)
            .put(DateTimes.of("2016-07-07T00:00:00.000Z"), 0.902389)
            .put(DateTimes.of("2016-07-08T00:00:00.000Z"), 0.903849)
            .put(DateTimes.of("2016-07-09T00:00:00.000Z"), 0.904593)
            .put(DateTimes.of("2016-07-10T00:00:00.000Z"), 0.904593)
            .put(DateTimes.of("2016-07-11T00:00:00.000Z"), 0.905133)
            .put(DateTimes.of("2016-07-12T00:00:00.000Z"), 0.90247)
            .put(DateTimes.of("2016-07-13T00:00:00.000Z"), 0.902739)
            .put(DateTimes.of("2016-07-14T00:00:00.000Z"), 0.899944)
            .put(DateTimes.of("2016-07-15T00:00:00.000Z"), 0.900341)
            .put(DateTimes.of("2016-07-16T00:00:00.000Z"), 0.905772)
            .put(DateTimes.of("2016-07-17T00:00:00.000Z"), 0.905772)
            .put(DateTimes.of("2016-07-18T00:00:00.000Z"), 0.903979)
            .put(DateTimes.of("2016-07-19T00:00:00.000Z"), 0.904945)
            .put(DateTimes.of("2016-07-20T00:00:00.000Z"), 0.908306)
            .put(DateTimes.of("2016-07-21T00:00:00.000Z"), 0.907408)
            .put(DateTimes.of("2016-07-22T00:00:00.000Z"), 0.908232)
            .put(DateTimes.of("2016-07-23T00:00:00.000Z"), 0.910631)
            .put(DateTimes.of("2016-07-24T00:00:00.000Z"), 0.910631)
            .put(DateTimes.of("2016-07-25T00:00:00.000Z"), 0.910896)
            .put(DateTimes.of("2016-07-26T00:00:00.000Z"), 0.90943)
            .put(DateTimes.of("2016-07-27T00:00:00.000Z"), 0.909124)
            .put(DateTimes.of("2016-07-28T00:00:00.000Z"), 0.902454)
            .put(DateTimes.of("2016-07-29T00:00:00.000Z"), 0.899434)
            .put(DateTimes.of("2016-07-30T00:00:00.000Z"), 0.894486)
            .put(DateTimes.of("2016-07-31T00:00:00.000Z"), 0.894486)
            .build()
    );
    doubleSumQuery = Druids.newTimeseriesQueryBuilder()
                           .dataSource("xxx")
                           .intervals("2016-07-01/P1M")
                           .aggregators(Collections.singletonList(new DoubleSumAggregatorFactory("usd", "usd")))
                           .build();
    currencyQuery = Druids.newTimeseriesQueryBuilder()
                          .dataSource("xxx")
                          .intervals("2016-07-01/P1M")
                          .aggregators(Collections.singletonList(aggregatorFactory))
                          .build();
    factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OperationsPerInvocation(NUM_ROWS)
  public void testCurrencySum(Blackhole blackhole)
  {
    final Sequence<Result<TimeseriesResultValue>> result = factory.createRunner(segment)
                                                                  .run(
                                                                      QueryPlus.wrap(currencyQuery),
                                                                      DefaultResponseContext.createEmpty()
                                                                  );
    blackhole.consume(result.toList());
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OperationsPerInvocation(NUM_ROWS)
  public void testDoubleSum(Blackhole blackhole)
  {
    final Sequence<Result<TimeseriesResultValue>> result = factory.createRunner(segment)
                                                                  .run(
                                                                      QueryPlus.wrap(doubleSumQuery),
                                                                      DefaultResponseContext.createEmpty()
                                                                  );
    blackhole.consume(result.toList());
  }
}
