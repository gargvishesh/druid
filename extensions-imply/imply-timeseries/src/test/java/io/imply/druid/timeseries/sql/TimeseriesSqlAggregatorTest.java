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
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.timeseries.TimeSeriesModule;
import io.imply.druid.timeseries.aggregation.DeltaTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.MeanTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.interpolation.Interpolator;
import io.imply.druid.timeseries.postaggregators.InterpolationPostAggregator;
import io.imply.druid.timeseries.postaggregators.TimeWeightedAveragePostAggregator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class TimeseriesSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    TimeSeriesModule timeseries = new TimeSeriesModule();
    timeseries.setImplyLicenseManager(new TestingImplyLicenseManager(null));

    return Iterables.concat(
        super.getJacksonModules(),
        timeseries.getJacksonModules());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    TimeSeriesModule timeseries = new TimeSeriesModule();
    timeseries.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    for (Module mod : timeseries.getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
      TestHelper.JSON_MAPPER.registerModule(mod);
    }
    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1")
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(CalciteTests.ROWS1)
                                             .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
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
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(
        ImmutableSet.of(
            new SimpleTimeSeriesObjectSqlAggregator(),
            new MeanDeltaTimeSeriesObjectSqlAggregator(MeanDeltaTimeSeriesObjectSqlAggregator.AggregatorType.MEAN_TIMESERIES),
            new MeanDeltaTimeSeriesObjectSqlAggregator(MeanDeltaTimeSeriesObjectSqlAggregator.AggregatorType.DELTA_TIMESERIES)
        ),
        ImmutableSet.of(
            new InterpolationOperatorConversion.LinearInterpolationOperatorConversion(),
            new InterpolationOperatorConversion.PaddingInterpolationOperatorConversion(),
            new InterpolationOperatorConversion.BackfillInterpolationOperatorConversion(),
            new TimeWeightedAverageOperatorConversion()
        )
    );
  }

  @Override
  public ObjectMapper createQueryJsonMapper()
  {
    ObjectMapper objectMapper = super.createQueryJsonMapper();
    objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return objectMapper;
  }

  @Test
  public void testTimeseriesAggs() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'),\n"
        + "  mean_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D'),\n"
        + "  delta_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D'),\n"
        + "  linear_interpolation(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'PT12H'),\n"
        + "  padding_interpolation(mean_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D', 100), 'PT12H'),\n"
        + "  time_weighted_average(timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z'), 'linear', 'P1D'), \n"
        + "  time_weighted_average(backfill_interpolation(delta_timeseries(__time, m1, '2000-01-01T00:00:00Z/2000-01-04T00:00:00Z', 'P1D', 100), 'PT12H'), 'linear', 'P1D')\n"
        + "FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory("a0:agg",
                                                                                        "m1",
                                                                                        "__time",
                                                                                        null,
                                                                                        null,
                                                                                        Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                                                                                        null),
                      MeanTimeSeriesAggregatorFactory.getMeanTimeSeriesAggregationFactory("a1:agg",
                                                                                          "m1",
                                                                                          "__time",
                                                                                          null,
                                                                                          null,
                                                                                          86400000L,
                                                                                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                                                                                          null),
                      DeltaTimeSeriesAggregatorFactory.getDeltaTimeSeriesAggregationFactory("a2:agg",
                                                                                           "m1",
                                                                                           "__time",
                                                                                           null,
                                                                                           null,
                                                                                            86400000L,
                                                                                           Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                                                                                           null),
                      MeanTimeSeriesAggregatorFactory.getMeanTimeSeriesAggregationFactory("a3:agg",
                                                                                          "m1",
                                                                                          "__time",
                                                                                          null,
                                                                                          null,
                                                                                          86400000L,
                                                                                          Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                                                                                          100),
                      DeltaTimeSeriesAggregatorFactory.getDeltaTimeSeriesAggregationFactory("a4:agg",
                                                                                            "m1",
                                                                                            "__time",
                                                                                            null,
                                                                                            null,
                                                                                            86400000L,
                                                                                            Intervals.of("2000-01-01T00:00:00Z/2000-01-04T00:00:00Z"),
                                                                                            100)
                  ))
                  .postAggregators(
                      new FieldAccessPostAggregator("p0", "a0:agg"),
                      new FieldAccessPostAggregator("p1", "a1:agg"),
                      new FieldAccessPostAggregator("p2", "a2:agg"),
                      new InterpolationPostAggregator(
                          "p4",
                          new FieldAccessPostAggregator("p3", "a0:agg"),
                          Interpolator.LINEAR,
                          12 * 60 * 60 * 1000L
                      ),
                      new InterpolationPostAggregator(
                          "p6",
                          new FieldAccessPostAggregator("p5", "a3:agg"),
                          Interpolator.PADDING,
                          12 * 60 * 60 * 1000L
                      ),
                      new TimeWeightedAveragePostAggregator(
                          "p8",
                          new FieldAccessPostAggregator("p7", "a0:agg"),
                          Interpolator.LINEAR,
                          86400000L),
                      new TimeWeightedAveragePostAggregator(
                          "p11",
                          new InterpolationPostAggregator(
                              "p10",
                              new FieldAccessPostAggregator("p9", "a4:agg"),
                              Interpolator.BACKFILL,
                              12 * 60 * 60 * 1000L
                          ),
                          Interpolator.LINEAR,
                          86400000L)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{"{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[1.0,2.0,3.0],"
                                          + "\"timestamps\":[946684800000,946771200000,946857600000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}}," // this result is wrong due to a bug,
                                          + "\"bucketStarts\":[946684800000,946771200000,946857600000]," // which leads to non-finalization of aggregations,
                                          + "\"countPoints\":[1,1,1],"
                                          + "\"sumPoints\":[1.0,2.0,3.0],"
                                          + "\"timeBucketGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                                          + "\"timeBucketGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                                          + "\"timestamps\":[946684800000,946684800000,946771200000,946771200000,946857600000,946857600000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[1.0,1.5,2.0,2.5,3.0,3.5],"
                                          + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[1.0,1.0,2.0,2.0,3.0,3.0],"
                                          + "\"timestamps\":[946684800000,946728000000,946771200000,946814400000,946857600000,946900800000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[1.5,2.5,3.5],"
                                          + "\"timestamps\":[946684800000,946771200000,946857600000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}",
                                      "{\"bounds\":{\"end\":{\"data\":null,\"timestamp\":null},\"start\":{\"data\":null,\"timestamp\":null}},"
                                          + "\"dataPoints\":[0.0,0.0,0.0],"
                                          + "\"timestamps\":[946684800000,946771200000,946857600000],"
                                          + "\"window\":\"2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z\"}"})
    );
  }
}
