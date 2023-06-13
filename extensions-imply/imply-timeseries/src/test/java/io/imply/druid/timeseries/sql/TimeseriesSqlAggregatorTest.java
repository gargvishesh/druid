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
import io.imply.druid.timeseries.aggregation.DeltaTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.MeanTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.expressions.InterpolationTimeseriesExprMacro;
import io.imply.druid.timeseries.expressions.MaxOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expressions.TimeWeightedAverageTimeseriesExprMacro;
import io.imply.druid.timeseries.expressions.TimeseriesToJSONExprMacro;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
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
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
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
        + "  timeseries_to_json(mean_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D')),\n"
        + "  timeseries_to_json(delta_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D')),\n"
        + "  timeseries_to_json(linear_interpolation(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')),\n"
        + "  timeseries_to_json(padding_interpolation(mean_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D', 100), 'PT12H')),\n"
        + "  timeseries_to_json(time_weighted_average(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'linear', 'P1D')), \n"
        + "  timeseries_to_json(time_weighted_average(backfill_interpolation(delta_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D', 100), 'PT12H'), 'linear', 'P1D')), \n"
        + "  timeseries_to_json(linear_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(padded_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(backfill_boundary(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H')), \n"
        + "  timeseries_to_json(sum_timeseries(ts)), \n"
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
                      MeanTimeSeriesAggregatorFactory.getMeanTimeSeriesAggregationFactory(
                          "a1:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          null
                      ),
                      DeltaTimeSeriesAggregatorFactory.getDeltaTimeSeriesAggregationFactory(
                          "a2:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          null
                      ),
                      MeanTimeSeriesAggregatorFactory.getMeanTimeSeriesAggregationFactory(
                          "a3:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          100
                      ),
                      DeltaTimeSeriesAggregatorFactory.getDeltaTimeSeriesAggregationFactory(
                          "a4:agg",
                          "m1",
                          "__time",
                          null,
                          86400000L,
                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                          100
                      ),
                      SumTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
                          "a5:agg",
                          "ts",
                          260_000
                      )
                  ))
                  .postAggregators(
                      new ExpressionPostAggregator(
                          "p0",
                          StringUtils.format("%s(\"a0:agg\")", TimeseriesToJSONExprMacro.NAME),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p1",
                          StringUtils.format("%s(\"a1:agg\")", TimeseriesToJSONExprMacro.NAME),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p2",
                          StringUtils.format("%s(\"a2:agg\")", TimeseriesToJSONExprMacro.NAME),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p3",
                          StringUtils.format(
                              "%s(%s(\"a0:agg\",'PT12H'))",
                              TimeseriesToJSONExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.LinearInterpolationTimeseriesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p4",
                          StringUtils.format(
                              "%s(%s(\"a3:agg\",'PT12H'))",
                              TimeseriesToJSONExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.PaddingInterpolationTimeseriesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p5",
                          StringUtils.format(
                              "%s(%s(\"a0:agg\",'linear','P1D'))",
                              TimeseriesToJSONExprMacro.NAME,
                              TimeWeightedAverageTimeseriesExprMacro.NAME
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p6",
                          StringUtils.format(
                              "%s(%s(%s(\"a4:agg\",'PT12H'),'linear','P1D'))",
                              TimeseriesToJSONExprMacro.NAME,
                              TimeWeightedAverageTimeseriesExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.BackfillInterpolationTimeseriesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p7",
                          StringUtils.format(
                              "%s(%s(\"a0:agg\",'PT12H'))",
                              TimeseriesToJSONExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.LinearInterpolationTimeseriesWithBoundariesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p8",
                          StringUtils.format(
                              "%s(%s(\"a0:agg\",'PT12H'))",
                              TimeseriesToJSONExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.PaddingInterpolationTimeseriesWithBoundariesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p9",
                          StringUtils.format(
                              "%s(%s(\"a0:agg\",'PT12H'))",
                              TimeseriesToJSONExprMacro.NAME,
                              new InterpolationTimeseriesExprMacro.BackfillInterpolationTimeseriesWithBoundariesExprMacro().name()
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p10",
                          StringUtils.format(
                              "%s(\"a5:agg\")",
                              TimeseriesToJSONExprMacro.NAME
                          ),
                          null,
                          Util.getMacroTable()
                      ),
                      new ExpressionPostAggregator(
                          "p11",
                          StringUtils.format("%s(\"a0:agg\")", MaxOverTimeseriesExprMacro.NAME),
                          null,
                          Util.getMacroTable()
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                // timeseries
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"dataPoints\":[1.0,2.0,3.0],"
                    + "\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // mean_timeseries
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"dataPoints\":[1.0,2.0,3.0],\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // delta_timeseries
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                    + "\"dataPoints\":[0.0,0.0,0.0],\"timestamps\":[946684800000,946771200000,946857600000],"
                    + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // linear_interpolation
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.0,1.5,2.0,2.5,3.0,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // padding_interpolation
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // time_weighted_average(timeseries)
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.5,2.5,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946771200000,946857600000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // time_weighted_average(backfill_interpolation)
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[0.0,0.0,3.0],"
                  + "\"timestamps\":[946684800000,946771200000,946857600000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // linear_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.0,1.5,2.0,2.5,3.0,3.0013736263736264],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // padded_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // backfill_boundary
                "{\"bounds\":{\"end\":{\"data\":4.0,\"timestamp\":978307200000},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[1.0,2.0,2.0,3.0,3.0,4.0],"
                  + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                  + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                // sum_timeseries
                "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                  + "\"dataPoints\":[21.0],\"timestamps\":[1],"
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
        "SELECT max_over_timeseries(sum_timeseries(ts)) FROM ( \n" +
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
                          SumTimeSeriesObjectSqlAggregator.SQL_DEFAULT_MAX_ENTRIES
                      )
                  ))
                  .setPostAggregatorSpecs(
                      ImmutableList.of(
                          new ExpressionPostAggregator(
                              "p0",
                              "max_over_timeseries(\"_a0:agg\")",
                              null,
                              Util.getMacroTable()
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
  public void testSumTimeseriesAggOuterQuery_MaxEntriesParameterFailure()
  {
    cannotVectorize();
    Assert.assertThrows(
        "Query not supported",
        UnsupportedSQLQueryException.class,
        () -> testQuery(
            "SELECT sum_timeseries(ts, 5) FROM ( \n" +
            "SELECT padded_boundary(timeseries('0', m1, '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z', 100), 'PT1H') as ts \n"
            + "FROM foo GROUP BY m1"
            + ")",
            ImmutableList.of(),
            ImmutableList.of()
    ));
  }

  @Test
  public void testTimeseriesExpressions()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  max_over_timeseries(ts) "
        + "FROM foo",
        Collections.singletonList(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          StringUtils.format("%s(\"ts\")", MaxOverTimeseriesExprMacro.NAME),
                          ColumnType.DOUBLE,
                          Util.getMacroTable()
                      )
                  )
                  .columns("v0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1D},
            new Object[]{2D},
            new Object[]{3D},
            new Object[]{4D},
            new Object[]{5D},
            new Object[]{6D}
        )
    );
  }
}
