/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.query.aggregation.datasketches.tuple.ImplyArrayOfDoublesSketchModule;
import io.imply.druid.query.aggregation.datasketches.tuple.SampledAvgScoreAggregatorFactory;
import io.imply.druid.query.aggregation.datasketches.tuple.SampledAvgScoreToHistogramPostAggregator;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchToEstimatePostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SampledAvgScoreObjectSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    ArrayOfDoublesSketchModule arrayOfDoublesSketch = new ArrayOfDoublesSketchModule();
    arrayOfDoublesSketch.configure(null);
    ImplyArrayOfDoublesSketchModule implyArrayOfDoublesSketchModule = new ImplyArrayOfDoublesSketchModule();
    implyArrayOfDoublesSketchModule.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    for (Module mod : Stream
        .concat(
            arrayOfDoublesSketch.getJacksonModules().stream(),
            implyArrayOfDoublesSketchModule.getJacksonModules().stream())
        .collect(Collectors.toList())) {
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
            new SampledAvgScoreObjectSqlAggregator()
        ),
        ImmutableSet.of(
            new SampledAvgScoreToEstimateOperatorConversion(),
            new SampledAvgScoreToHistogramOperatorConversion()
        )
    );
  }

  @Test
  public void testPostAggs() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  SAMPLED_AVG_SCORE_ESTIMATE(SAMPLED_AVG_SCORE(dim1, m1)),\n"
        + "  SAMPLED_AVG_SCORE_ESTIMATE(SAMPLED_AVG_SCORE(dim1, m1, 100)),\n"
        + "  SAMPLED_AVG_SCORE_HISTOGRAM(SAMPLED_AVG_SCORE(dim1, m1), 3.5),\n"
        + "  SAMPLED_AVG_SCORE_HISTOGRAM(SAMPLED_AVG_SCORE(dim1, m1, 100), 3.5),\n"
        + "  SAMPLED_AVG_SCORE_HISTOGRAM(SAMPLED_AVG_SCORE(m1 * 2, m1 * 3, 100), 7)\n"
        + "FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"m1\" * 2)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "(\"m1\" * 3)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new LongSumAggregatorFactory("a0", "cnt"),
                      new SampledAvgScoreAggregatorFactory("a1:agg",
                                                           "dim1",
                                                           "m1",
                                                           SampledAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES),
                      new SampledAvgScoreAggregatorFactory("a2:agg",
                                                           "dim1",
                                                           "m1",
                                                           128),
                      new SampledAvgScoreAggregatorFactory("a3:agg",
                                                           "v0",
                                                           "v1",
                                                           128)
                  ))
                  .postAggregators(
                      new ArrayOfDoublesSketchToEstimatePostAggregator(
                          "p1",
                          new FieldAccessPostAggregator("p0", "a1:agg")
                      ),
                      new ArrayOfDoublesSketchToEstimatePostAggregator(
                          "p3",
                          new FieldAccessPostAggregator("p2", "a2:agg")
                      ),
                      new SampledAvgScoreToHistogramPostAggregator(
                          "p5",
                          new FieldAccessPostAggregator("p4", "a1:agg"),
                          new double[]{3.5}
                      ),
                      new SampledAvgScoreToHistogramPostAggregator(
                          "p7",
                          new FieldAccessPostAggregator("p6", "a2:agg"),
                          new double[]{3.5}
                      ),
                      new SampledAvgScoreToHistogramPostAggregator(
                          "p9",
                          new FieldAccessPostAggregator("p8", "a3:agg"),
                          new double[]{7}
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[] {
            6L,
            5.0D,
            5.0D,
            "[2,3]",
            "[2,3]",
            "[2,4]"
        }));
  }
}
