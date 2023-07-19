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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.imply.druid.query.aggregation.datasketches.expressions.MurmurHashExprMacros;
import io.imply.druid.query.aggregation.datasketches.expressions.SessionizeExprMacro;
import io.imply.druid.query.aggregation.datasketches.tuple.ImplyArrayOfDoublesSketchModule;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreAggregatorFactory;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreSummaryStatsPostAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreToHistogramFilteringPostAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionAvgScoreToHistogramPostAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.SessionSampleRatePostAggregator;
import io.imply.druid.query.aggregation.datasketches.virtual.ImplySessionFilteringVirtualColumn;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchToEstimatePostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SessionAvgScoreTest extends BaseCalciteQueryTest
{
  private static List<ExprMacroTable.ExprMacro> macros;

  @BeforeClass
  public static void setup()
  {
    macros = CalciteTests.createExprMacroTable().getMacros();
    macros = new ArrayList<>(macros);
    macros.add(new MurmurHashExprMacros.Murmur3Macro());
    macros.add(new MurmurHashExprMacros.Murmur3_64Macro());
    macros.add(new SessionizeExprMacro());
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    ArrayOfDoublesSketchModule arrayOfDoublesSketch = new ArrayOfDoublesSketchModule();
    ArrayOfDoublesSketchModule.registerSerde();
    super.configureGuice(builder);
    builder.addModules(arrayOfDoublesSketch, new ImplyArrayOfDoublesSketchModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      JoinableFactoryWrapper joinableFactory,
      Injector injector
  ) throws IOException
  {
    ArrayOfDoublesSketchModule arrayOfDoublesSketch = new ArrayOfDoublesSketchModule();
    ArrayOfDoublesSketchModule.registerSerde();
    ImplyArrayOfDoublesSketchModule implyArrayOfDoublesSketchModule = new ImplyArrayOfDoublesSketchModule();
    for (Module mod : Stream
        .concat(
            arrayOfDoublesSketch.getJacksonModules().stream(),
            implyArrayOfDoublesSketchModule.getJacksonModules().stream())
        .collect(Collectors.toList())) {
      CalciteTests.getJsonMapper().registerModule(mod);
      TestHelper.JSON_MAPPER.registerModule(mod);
    }
    final QueryableIndex index = IndexBuilder
        .create()
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
        .rows(TestDataBuilder.ROWS1)
        .buildMMappedIndex();

    final QueryableIndex visitIndex = IndexBuilder
        .create()
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
        .rows(TestDataBuilder.USER_VISIT_ROWS)
        .buildMMappedIndex();


    SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate)
        .add(
            DataSegment.builder()
                       .dataSource(CalciteTests.DATASOURCE1)
                       .interval(index.getDataInterval())
                       .version("1")
                       .shardSpec(new LinearShardSpec(0))
                       .size(0)
                       .build(),
            index
        ).add(
            DataSegment.builder()
                       .dataSource(CalciteTests.USERVISITDATASOURCE)
                       .interval(visitIndex.getDataInterval())
                       .version("1")
                       .shardSpec(new LinearShardSpec(0))
                       .size(0)
                       .build(),
            visitIndex
        );
    return walker;
  }

  @Test
  public void testPostAggs()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  SESSION_AVG_SCORE_ESTIMATE(SESSION_AVG_SCORE(dim1, m1)),\n"
        + "  SESSION_AVG_SCORE_ESTIMATE(SESSION_AVG_SCORE(dim1, m1, 100)),\n"
        + "  SESSION_AVG_SCORE_HISTOGRAM(SESSION_AVG_SCORE(dim1, m1), 3.5),\n"
        + "  SESSION_AVG_SCORE_HISTOGRAM(SESSION_AVG_SCORE(dim1, m1, 100), 3.5),\n"
        + "  SESSION_AVG_SCORE_HISTOGRAM(SESSION_AVG_SCORE(m1 * 2, m1 * 3, 100), 7),\n"
        + "  SESSION_AVG_SCORE_HISTOGRAM_FILTERING(SESSION_AVG_SCORE(dim1, m1), ARRAY[3.5], ARRAY[1]),\n"
        + "  SESSION_AVG_SCORE_HISTOGRAM(SESSION_AVG_SCORE(dim1, m1 - 2, true), 1),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'n'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'mean'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'max'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'min'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'sum'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'geometric_mean'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'variance'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'population_variance'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'second_moment'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'sum_of_squares'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'std_deviation'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'n'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'mean'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'max'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'min'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'sum'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'geometric_mean'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'variance'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'population_variance'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'second_moment'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'sum_of_squares'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(dim1, m1), 'std_deviation'),\n"
        + "  SESSION_SAMPLE_RATE(SESSION_AVG_SCORE(dim1, m1)) "
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
                      ),
                      new ExpressionVirtualColumn(
                          "v2",
                          "(\"m1\" - 2)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new LongSumAggregatorFactory("a0", "cnt"),
                      new SessionAvgScoreAggregatorFactory("a1:agg",
                                                           "dim1",
                                                           "m1",
                                                           SessionAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES,
                                                           false),
                      new SessionAvgScoreAggregatorFactory("a2:agg",
                                                           "dim1",
                                                           "m1",
                                                           128,
                                                           false),
                      new SessionAvgScoreAggregatorFactory("a3:agg",
                                                           "v0",
                                                           "v1",
                                                           128,
                                                           false),
                      new SessionAvgScoreAggregatorFactory("a4:agg",
                                                           "dim1",
                                                           "v2",
                                                           SessionAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES,
                                                           true)
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
                      new SessionAvgScoreToHistogramPostAggregator(
                          "p5",
                          new FieldAccessPostAggregator("p4", "a1:agg"),
                          new double[]{3.5}
                      ),
                      new SessionAvgScoreToHistogramPostAggregator(
                          "p7",
                          new FieldAccessPostAggregator("p6", "a2:agg"),
                          new double[]{3.5}
                      ),
                      new SessionAvgScoreToHistogramPostAggregator(
                          "p9",
                          new FieldAccessPostAggregator("p8", "a3:agg"),
                          new double[]{7}
                      ),
                      new SessionAvgScoreToHistogramFilteringPostAggregator(
                          "p11",
                          new FieldAccessPostAggregator("p10", "a1:agg"),
                          new double[]{3.5},
                          new int[]{1}
                      ),
                      new SessionAvgScoreToHistogramPostAggregator(
                          "p13",
                          new FieldAccessPostAggregator("p12", "a4:agg"),
                          new double[]{1}
                      ),
                      makeStatsPostAgg(14, "a1:agg", "n", true),
                      makeStatsPostAgg(16, "a1:agg", "mean", true),
                      makeStatsPostAgg(18, "a1:agg", "max", true),
                      makeStatsPostAgg(20, "a1:agg", "min", true),
                      makeStatsPostAgg(22, "a1:agg", "sum", true),
                      makeStatsPostAgg(24, "a1:agg", "geometric_mean", true),
                      makeStatsPostAgg(26, "a1:agg", "variance", true),
                      makeStatsPostAgg(28, "a1:agg", "population_variance", true),
                      makeStatsPostAgg(30, "a1:agg", "second_moment", true),
                      makeStatsPostAgg(32, "a1:agg", "sum_of_squares", true),
                      makeStatsPostAgg(34, "a1:agg", "std_deviation", true),
                      makeStatsPostAgg(36, "a1:agg", "n", false),
                      makeStatsPostAgg(38, "a1:agg", "mean", false),
                      makeStatsPostAgg(40, "a1:agg", "max", false),
                      makeStatsPostAgg(42, "a1:agg", "min", false),
                      makeStatsPostAgg(44, "a1:agg", "sum", false),
                      makeStatsPostAgg(46, "a1:agg", "geometric_mean", false),
                      makeStatsPostAgg(48, "a1:agg", "variance", false),
                      makeStatsPostAgg(50, "a1:agg", "population_variance", false),
                      makeStatsPostAgg(52, "a1:agg", "second_moment", false),
                      makeStatsPostAgg(54, "a1:agg", "sum_of_squares", false),
                      makeStatsPostAgg(56, "a1:agg", "std_deviation", false),
                      new SessionSampleRatePostAggregator("p59", new FieldAccessPostAggregator("p58", "a1:agg"))
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[] {
            6L, 5D, 5D, "[2,3]", "[2,3]", "[2,4]", "ZauJNuxDxpB9JU1IDrmw8QnDIUw7B9NV", "[1,4]",
            5D, 4D, 6D, 2D, 20D, 3.7279192731913513D, 2.5D, 2D, 10D, 90D, 1.5811388300841898D,
            5D, 4D, 6D, 2D, 20D, 3.7279192731913513D, 2.5D, 2D, 10D, 90D, 1.5811388300841898D,
            1D,
        }));
  }

  @Test
  public void testSummaryStatsPostAggs()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'n'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'mean'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'max'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'min'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'sum'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'geometric_mean'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'variance'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'population_variance'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'second_moment'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'sum_of_squares'),\n"
        + "  SESSION_AVG_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'std_deviation'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'n'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'mean'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'max'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'min'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'sum'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'geometric_mean'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'variance'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'population_variance'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'second_moment'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'sum_of_squares'),\n"
        + "  SESSION_SCORE_STATS(SESSION_AVG_SCORE(user, 1), 'std_deviation')\n"
        + "FROM visits",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("visits")
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "1",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new SessionAvgScoreAggregatorFactory(
                          "a0:agg",
                          "user",
                          "v0",
                          SessionAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES,
                          false
                      )
                  ))
                  .postAggregators(
                      makeStatsPostAgg(0, "a0:agg", "n", true),
                      makeStatsPostAgg(2, "a0:agg", "mean", true),
                      makeStatsPostAgg(4, "a0:agg", "max", true),
                      makeStatsPostAgg(6, "a0:agg", "min", true),
                      makeStatsPostAgg(8, "a0:agg", "sum", true),
                      makeStatsPostAgg(10, "a0:agg", "geometric_mean", true),
                      makeStatsPostAgg(12, "a0:agg", "variance", true),
                      makeStatsPostAgg(14, "a0:agg", "population_variance", true),
                      makeStatsPostAgg(16, "a0:agg", "second_moment", true),
                      makeStatsPostAgg(18, "a0:agg", "sum_of_squares", true),
                      makeStatsPostAgg(20, "a0:agg", "std_deviation", true),
                      makeStatsPostAgg(22, "a0:agg", "n", false),
                      makeStatsPostAgg(24, "a0:agg", "mean", false),
                      makeStatsPostAgg(26, "a0:agg", "max", false),
                      makeStatsPostAgg(28, "a0:agg", "min", false),
                      makeStatsPostAgg(30, "a0:agg", "sum", false),
                      makeStatsPostAgg(32, "a0:agg", "geometric_mean", false),
                      makeStatsPostAgg(34, "a0:agg", "variance", false),
                      makeStatsPostAgg(36, "a0:agg", "population_variance", false),
                      makeStatsPostAgg(38, "a0:agg", "second_moment", false),
                      makeStatsPostAgg(40, "a0:agg", "sum_of_squares", false),
                      makeStatsPostAgg(42, "a0:agg", "std_deviation", false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[] {
            4D, 1D, 1D, 1D, 4D, 1D, 0D, 0D, 0D, 4D, 0D,
            4D, 3D, 5D, 1D, 12D, 2.5900200641113513D, 2.6666666666666665D, 2D, 8D, 44D, 1.632993161855452D
        }));
  }

  @Test
  public void testMurmurFunctions()
  {
    testQuery(
        "SELECT\n"
        + "  ds_utf8_murmur3(m1),\n"
        + "  ds_utf8_murmur3_64(m1)\n"
        + "FROM foo",
        Collections.singletonList(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "ds_utf8_murmur3(\"m1\")",
                          ColumnType.STRING,
                          new ExprMacroTable(macros)
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "ds_utf8_murmur3_64(\"m1\")",
                          ColumnType.LONG,
                          new ExprMacroTable(macros)
                      )
                  )
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .columns("v0", "v1")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[] {
                new String(new byte[]{-109, 96, 127, -31, -20, -26, -43, -40, 58, -23, -100, 29, 109, -119, 90, -120}, StandardCharsets.UTF_8),
                7812324193650847817L
            },
            new Object[] {
                new String(new byte[]{111, -42, 5, -30, 119, -40, -97, 86, -97, 93, -72, -5, -7, -103, -50, -25}, StandardCharsets.UTF_8),
                3120972808981769015L
            },
            new Object[] {
                new String(new byte[]{-113, 99, 71, -3, 103, -23, -60, -128, -126, 107, 77, 6, 94, -52, 107, -30}, StandardCharsets.UTF_8),
                4639398882565140935L
            },
            new Object[] {
                new String(new byte[]{46, -128, -9, -69, -118, 70, 112, -102, 107, 50, 53, -96, 38, -13, -51, -58}, StandardCharsets.UTF_8),
                5564236120452743191L
            },
            new Object[] {
                new String(new byte[]{5, 63, 26, -70, 77, -98, -15, 77, -69, -65, -71, -42, -24, 117, -92, 25}, StandardCharsets.UTF_8),
                2808222133489835906L
            },
            new Object[] {
                new String(new byte[]{-17, 108, -97, 52, -55, 59, 68, 69, 33, -40, 118, -1, 110, 119, 106, -116}, StandardCharsets.UTF_8),
                2495590011195340407L
            }
        )
    );
  }

  @Test
  public void testSessionFilteringVirtualColumn()
  {
    ImplySessionFilteringVirtualColumn virtualColumn = new ImplySessionFilteringVirtualColumn("v0", "dim1");
    virtualColumn.getFilterValues().set(new HashSet<>(ImmutableList.of(7326100087833347728L, 9017698800759320817L, 703442578091529045L)));
    testQuery(
        "SELECT\n"
        + "  dim1\n"
        + " FROM foo\n"
        + " WHERE sessionize(dim1) = 'ZauJNuxDxpB9JU1IDrmw8QnDIUw7B9NV'",
        Collections.singletonList(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .virtualColumns(virtualColumn)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .columns("dim1")
                  .filters(equality("v0", "ZauJNuxDxpB9JU1IDrmw8QnDIUw7B9NV", ColumnType.STRING))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"1"},
            new Object[]{"def"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSessionFilteringVirtualColumnInJoin()
  {
    cannotVectorize();

    ImmutableMap.Builder<String, Object> queryContextBuilder = ImmutableMap.builder();
    queryContextBuilder.putAll(QUERY_CONTEXT_DEFAULT);
    queryContextBuilder.put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true);

    ImplySessionFilteringVirtualColumn virtualColumn = new ImplySessionFilteringVirtualColumn("v0", "dim1");
    virtualColumn.getFilterValues().set(new HashSet<>(ImmutableList.of(7326100087833347728L, 9017698800759320817L, 703442578091529045L)));
    testQuery(
        "SELECT\n"
        + "  dim1\n"
        + " FROM foo\n"
        + " WHERE sessionize(dim1) IN (SELECT SESSION_AVG_SCORE_HISTOGRAM_FILTERING(SESSION_AVG_SCORE(dim1, m1), ARRAY[3.5], ARRAY[1]) from foo)",
        queryContextBuilder.build(),
        Collections.singletonList(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                  .granularity(Granularities.ALL)
                                  .aggregators(
                                      ImmutableList.of(new SessionAvgScoreAggregatorFactory("a0:agg",
                                                                                            "dim1",
                                                                                            "m1",
                                                                                            SessionAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES,
                                                                                            false))
                                  )
                                  .postAggregators(
                                      new SessionAvgScoreToHistogramFilteringPostAggregator(
                                          "p1",
                                          new FieldAccessPostAggregator("p0", "a0:agg"),
                                          new double[]{3.5},
                                          new int[]{1}
                                      )
                                  )
                                  .context(queryContextBuilder.build())
                                  .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.p1")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(virtualColumn)
                .columns("dim1")
                .context(queryContextBuilder.build())
                .build()
        ),
        ImmutableList.of(
            new Object[]{"1"},
            new Object[]{"def"},
            new Object[]{"abc"}
        )
    );
  }

  @Nonnull
  private SessionAvgScoreSummaryStatsPostAggregator makeStatsPostAgg(
      int startId,
      String inputAgg,
      String statType,
      boolean useAverage
  )
  {
    return new SessionAvgScoreSummaryStatsPostAggregator(
        StringUtils.format("p%s", startId + 1),
        new FieldAccessPostAggregator(StringUtils.format("p%s", startId), inputAgg),
        SessionAvgScoreSummaryStatsPostAggregator.StatType.valueOf(statType.toUpperCase(Locale.ROOT)),
        useAverage
    );
  }
}
