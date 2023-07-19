/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.Util;
import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.expression.TimeseriesToJSONExprMacro;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class TimeseriesSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModules(new TimeSeriesModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      JoinableFactoryWrapper joinableFactory,
      Injector injector
  ) throws IOException
  {
    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    new TimeSeriesModule().getJacksonModules().forEach(objectMapper::registerModule);
    final QueryableIndex index = IndexBuilder.create(objectMapper)
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withVirtualColumns(VirtualColumns.create(
                                                         ImmutableList.of(
                                                             new ExpressionVirtualColumn(
                                                                 "v0",
                                                                 "1",
                                                                 ColumnType.LONG,
                                                                 ExprMacroTable.nil()
                                                             )
                                                         )
                                                     ))
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                                                             "ts",
                                                             "m1",
                                                             "v0",
                                                             null,
                                                             Intervals.ETERNITY,
                                                             null
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(TestDataBuilder.ROWS1)
                                             .buildMMappedIndex();

    SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
    return walker;
  }

  @Override
  public void configureJsonMapper(ObjectMapper queryJsonMapper)
  {
    queryJsonMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    queryJsonMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  @Test
  public void testTimeseriesAggs()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  timeseries_to_json(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z')),\n"
        + "  timeseries_to_json(downsampled_sum_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D')),\n"
        + "  timeseries_to_json(delta_timeseries(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'P1D')),\n"
        + "  timeseries_to_json(linear_interpolation(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')),\n"
        + "  timeseries_to_json(padding_interpolation(downsampled_sum_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D', 100), 'PT12H')),\n"
        + "  timeseries_to_json(time_weighted_average(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'linear', 'P1D')), \n"
        + "  timeseries_to_json(time_weighted_average(backfill_interpolation(delta_timeseries(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 100), 'P1D'), 'PT12H'), 'linear', 'P1D')), \n"
        + "  timeseries_to_json(linear_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(padded_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(backfill_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(sum_timeseries(ts, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z')), \n"
        + "  max_over_timeseries(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'))"
        + "FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                          "a0:agg",
                          "m1",
                          "__time",
                          null,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          null
                      ),
                      DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                          "a1:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          null
                      ),
                      DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                          "a2:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          100
                      ),
                      SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                          "a3:agg",
                          "m1",
                          "__time",
                          null,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          100
                      ),
                      SumTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                          "a4:agg",
                          "ts",
                          260_000,
                          Intervals.ETERNITY
                      )
                  ))
                  .postAggregators(
                      new ExpressionPostAggregator(
                          "p0",
                          "timeseries_to_json(\"a0:agg\")",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p1",
                          "timeseries_to_json(\"a1:agg\")",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p2",
                          "timeseries_to_json(delta_timeseries(\"a0:agg\",'P1D'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p3",
                          "timeseries_to_json(linear_interpolation(\"a0:agg\",'PT12H'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p4",
                          "timeseries_to_json(padding_interpolation(\"a2:agg\",'PT12H'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p5",
                          "timeseries_to_json(time_weighted_average(\"a0:agg\",'linear','P1D'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p6",
                          "timeseries_to_json(time_weighted_average(backfill_interpolation"
                          + "(delta_timeseries(\"a3:agg\",'P1D'),'PT12H'),'linear','P1D'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p7",
                          "timeseries_to_json(linear_boundary(\"a0:agg\",'PT12H'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p8",
                          "timeseries_to_json(padded_boundary(\"a0:agg\",'PT12H'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p9",
                          "timeseries_to_json(backfill_boundary(\"a0:agg\",'PT12H'))",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p10",
                          "timeseries_to_json(\"a4:agg\")",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p11",
                          "max_over_timeseries(\"a0:agg\")",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                // timeseries
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"bucketMillis\":1,\"dataPoints\":[1.0,2.0,3.0],"
                    + "\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // mean_timeseries
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"bucketMillis\":1,\"dataPoints\":[1.0,2.0,3.0],\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // delta_timeseries
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"bucketMillis\":86400000,\"dataPoints\":[1.0,1.0,1.0],\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // linear_interpolation
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[1.0,1.5,2.0,2.5,3.0,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // padding_interpolation
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // time_weighted_average(timeseries)
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":86400000,\"dataPoints\":[1.5,2.5,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946771200000,946857600000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // time_weighted_average(backfill_interpolation)
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":86400000,\"dataPoints\":[1.0,1.0,1.0],"
                  + "\"timestamps\":[946684800000,946771200000,946857600000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // linear_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":43200000,\"dataPoints\":[1.0,1.5,2.0,2.5,3.0,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // padded_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":43200000,\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // backfill_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":43200000,\"dataPoints\":[1.0,2.0,2.0,3.0,3.0,4.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // sum_timeseries
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[21.0],\"timestamps\":[1],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
                // max_over_timeseries
                3D
            })
    );
  }

  @Test
  public void testSumTimeseriesAggOuterQuery()
  {
    cannotVectorize();
    testQuery(
        "SELECT max_over_timeseries(sum_timeseries(ts, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z', 100)) FROM ( \n" +
        "SELECT timeseries('0', m1, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z') as ts \n"
        + "FROM foo GROUP BY m1"
        + ")",
        Collections.singletonList(
            new GroupByQuery.Builder()
                  .setDataSource(new QueryDataSource(
                      new GroupByQuery.Builder()
                          .setDataSource(CalciteTests.DATASOURCE1)
                          .setInterval(querySegmentSpec(Filtration.eternity()))
                          .setGranularity(Granularities.ALL)
                          .setVirtualColumns(
                              new ExpressionVirtualColumn(
                                  "v0",
                                  "'0'",
                                  ColumnType.LONG,
                                  ExprMacroTable.nil()
                              )
                          )
                          .setDimensions(
                              new DefaultDimensionSpec("m1", "d0", ColumnType.DOUBLE)
                          )
                          .setAggregatorSpecs(aggregators(
                              SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                                  "a0:agg",
                                  "m1",
                                  "v0",
                                  null,
                                  Intervals.ETERNITY,
                                  null
                              )
                          ))
                          .setContext(QUERY_CONTEXT_DEFAULT)
                          .build()
                  ))
                  .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .setGranularity(Granularities.ALL)
                  .setAggregatorSpecs(ImmutableList.of(
                      SumTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                          "_a0:agg",
                          "a0:agg",
                          100,
                          Intervals.ETERNITY
                      )
                  ))
                  .setPostAggregatorSpecs(
                      ImmutableList.of(
                          new ExpressionPostAggregator(
                              "p0",
                              "max_over_timeseries(\"_a0:agg\")",
                              null,
                              Util.makeTimeSeriesMacroTable()
                          )
                      )
                  )
                  .setContext(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{21.0D})
    );
  }

  @Test
  public void testTimeseriesExpressions()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  max_over_timeseries(ts),"
        + "  timeseries_to_json(delta_timeseries(ts)) "
        + "FROM foo",
        Collections.singletonList(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "max_over_timeseries(\"ts\")",
                          ColumnType.DOUBLE,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "timeseries_to_json(delta_timeseries(\"ts\"))",
                          TimeseriesToJSONExprMacro.TYPE,
                          Util.makeTimeSeriesMacroTable()
                      )
                  )
                  .columns("v0", "v1")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                1D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            },
            new Object[]{
                2D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            },
            new Object[]{
                3D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            },
            new Object[]{
                4D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            },
            new Object[]{
                5D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            },
            new Object[]{
                6D,
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[],\"timestamps\":[],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            }
        )
    );
  }

  @Test
  public void testArithmeticOverTimeseriesExpressions()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  timeseries_to_json(add_timeseries(ts, ts)), "
        + "  timeseries_to_json(subtract_timeseries(ts, ts)), "
        + "  timeseries_to_json(multiply_timeseries(ts, ts)), "
        + "  timeseries_to_json(divide_timeseries(ts, ts)) "
        + "FROM foo where m1 = 1",
        Collections.singletonList(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "timeseries_to_json(add_timeseries(\"ts\",\"ts\"))",
                          TimeseriesToJSONExprMacro.TYPE,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "timeseries_to_json(subtract_timeseries(\"ts\",\"ts\"))",
                          TimeseriesToJSONExprMacro.TYPE,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionVirtualColumn(
                          "v2",
                          "timeseries_to_json(multiply_timeseries(\"ts\",\"ts\"))",
                          TimeseriesToJSONExprMacro.TYPE,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionVirtualColumn(
                          "v3",
                          "timeseries_to_json(divide_timeseries(\"ts\",\"ts\"))",
                          TimeseriesToJSONExprMacro.TYPE,
                          Util.makeTimeSeriesMacroTable()
                      )
                  )
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? selector("m1", "1")
                      : equality("m1", 1.0, ColumnType.DOUBLE))
                  .columns("v0", "v1", "v2", "v3")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[2.0],\"timestamps\":[1],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[0.0],\"timestamps\":[1],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[1.0],\"timestamps\":[1],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"bucketMillis\":1,\"dataPoints\":[1.0],\"timestamps\":[1],"
                  + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}"
            }
        )
    );
  }

  @Test
  public void testArithmeticOverTimeseriesExpressions2()
  {
    cannotVectorize();
    testQuery(
        "SELECT timeseries_to_json(add_timeseries(ts, ts1)), timeseries_to_json(add_timeseries(ts, ts2)), timeseries_to_json(add_timeseries(ts, ts2, 'true')) FROM ( \n" +
        "SELECT timeseries('0', m1, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z') FILTER (where m1 = 1) as ts, \n" +
        "timeseries('0', m1, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z') FILTER (where m1 = 2) as ts1, \n" +
        "timeseries('0', m1, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z') FILTER (where m1 = -1) as ts2 \n"
        + "FROM foo"
        + ")",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .virtualColumns(
                    new ExpressionVirtualColumn(
                        "v0",
                        "'0'",
                        ColumnType.LONG,
                        ExprMacroTable.nil()
                    )
                )
                .aggregators(aggregators(
                    new FilteredAggregatorFactory(
                        SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                            "a0:agg",
                            "m1",
                            "v0",
                            null,
                            Intervals.ETERNITY,
                            null
                        ),
                        NullHandling.replaceWithDefault()
                        ? selector("m1", "1")
                        : equality("m1", 1.0, ColumnType.DOUBLE)
                    ),
                    new FilteredAggregatorFactory(
                        SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                            "a1:agg",
                            "m1",
                            "v0",
                            null,
                            Intervals.ETERNITY,
                            null
                        ),
                        NullHandling.replaceWithDefault()
                        ? selector("m1", "2")
                        : equality("m1", 2.0, ColumnType.DOUBLE)
                    ),
                    new FilteredAggregatorFactory(
                        SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                            "a2:agg",
                            "m1",
                            "v0",
                            null,
                            Intervals.ETERNITY,
                            null
                        ),
                        NullHandling.replaceWithDefault()
                        ? selector("m1", "-1")
                        : equality("m1", -1.0, ColumnType.DOUBLE)
                    )
                ))
                .postAggregators(ImmutableList.of(
                    new ExpressionPostAggregator(
                        "p0",
                        "timeseries_to_json(add_timeseries(\"a0:agg\",\"a1:agg\"))",
                        null,
                        Util.makeTimeSeriesMacroTable()
                    ),
                    new ExpressionPostAggregator(
                        "p1",
                        "timeseries_to_json(add_timeseries(\"a0:agg\",\"a2:agg\"))",
                        null,
                        Util.makeTimeSeriesMacroTable()
                    ),
                    new ExpressionPostAggregator(
                        "p2",
                        "timeseries_to_json(add_timeseries(\"a0:agg\",\"a2:agg\",'true'))",
                        null,
                        Util.makeTimeSeriesMacroTable()
                    )
                ))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{
            "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
              + "\"bucketMillis\":1,\"dataPoints\":[3.0],\"timestamps\":[0],"
              + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
            "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
              + "\"bucketMillis\":1,\"dataPoints\":[1.0],\"timestamps\":[0],"
              + "\"window\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"}",
            null
        })
    );
  }

  @Test
  public void testIRRExpressions()
  {
    cannotVectorize();
    testQuery(
        "SELECT IRR_DEBUG(__time, m1, m1, '2000-01-01T00:00:00Z/2000-01-03T00:00:00Z', 'P1D'),"
        + "  IRR(__time, m1, m1, '2000-01-01T00:00:00Z/2000-01-03T00:00:00Z', 'P1D') FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "timestamp_floor(\"__time\",'P1D')",
                          ColumnType.LONG,
                          Util.makeTimeSeriesMacroTable()
                      )
                  )
                  .aggregators(aggregators(
                      DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                          "a0:downsampledSum",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-03T00:00:00Z"),
                          2
                      ),
                      new FilteredAggregatorFactory(
                          new DoubleSumAggregatorFactory(
                              "a0:startInvestmentInternal",
                              "m1"
                          ),
                          equality("v0", 946684800000L, ColumnType.LONG),
                          "a0:startInvestment"
                      ),
                      new FilteredAggregatorFactory(
                          new DoubleSumAggregatorFactory(
                              "a0:endInvestmentInternal",
                              "m1"
                          ),
                          equality("v0", 946857600000L, ColumnType.LONG),
                          "a0:endInvestment"
                      ),
                      DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
                          "a1:downsampledSum",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-03T00:00:00Z"),
                          2
                      ),
                      new FilteredAggregatorFactory(
                          new DoubleSumAggregatorFactory(
                              "a1:startInvestmentInternal",
                              "m1"
                          ),
                          equality("v0", 946684800000L, ColumnType.LONG),
                          "a1:startInvestment"
                      ),
                      new FilteredAggregatorFactory(
                          new DoubleSumAggregatorFactory(
                              "a1:endInvestmentInternal",
                              "m1"
                          ),
                          equality("v0", 946857600000L, ColumnType.LONG),
                          "a1:endInvestment"
                      )
                  ))
                  .postAggregators(ImmutableList.of(
                      new ExpressionPostAggregator(
                          "a0",
                          "IRR_DEBUG(\"a0:downsampledSum\",\"a0:startInvestment\",\"a0:endInvestment\",'2000-01-01T00:00:00.000Z/2000-01-03T00:00:00.000Z')",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "a1",
                          "IRR(\"a1:downsampledSum\",\"a1:startInvestment\",\"a1:endInvestment\",'2000-01-01T00:00:00.000Z/2000-01-03T00:00:00.000Z')",
                          null,
                          Util.makeTimeSeriesMacroTable()
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "{\"cashFlows\":\"SimpleTimeSeries{timestamps=[946684800000, 946771200000], dataPoints=[1.0, 2.0], "
                  + "maxEntries=2, start=EdgePoint{timestamp=-1, data=-1.0}, end=EdgePoint{timestamp=946857600000, data=3.0}, "
                  + "bucketMillis=1}\",\"endValue\":\"3.0\","
                  + "\"iterations\":[{\"estimate\":\"-0.631542609054844\",\"iteration\":\"1\",\"npv\":\"1.1796524512184576\",\"npvDerivative\":\"1.612554670933759\"},"
                  + "{\"estimate\":\"-0.47001347181852526\",\"iteration\":\"2\",\"npv\":\"-2.847198672998438\",\"npvDerivative\":\"17.6265330312076\"},"
                  + "{\"estimate\":\"-0.35710585080926505\",\"iteration\":\"3\",\"npv\":\"-0.9132750459247614\",\"npvDerivative\":\"8.088692665394646\"},"
                  + "{\"estimate\":\"-0.3247598000634898\",\"iteration\":\"4\",\"npv\":\"-0.1720315792891718\",\"npvDerivative\":\"5.3184724355149005\"},"
                  + "{\"estimate\":\"-0.3228813920483182\",\"iteration\":\"5\",\"npv\":\"-0.00897397928101018\",\"npvDerivative\":\"4.777438771837062\"},"
                  + "{\"estimate\":\"-0.3228756555854865\",\"iteration\":\"6\",\"npv\":\"-2.723946764771057E-5\",\"npvDerivative\":\"4.7484780162460245\"},"
                  + "{\"estimate\":\"-0.32287565553229536\",\"iteration\":\"7\",\"npv\":\"-2.525721853885443E-10\",\"npvDerivative\":\"4.7483899579802475\"}],"
                  + "\"startEstimate\":\"0.1\",\"startValue\":\"1.0\",\"window\":\"2000-01-01T00:00:00.000Z/2000-01-03T00:00:00.000Z\"}",
                -0.32287565553229536D
            }
        )
    );
  }
}
