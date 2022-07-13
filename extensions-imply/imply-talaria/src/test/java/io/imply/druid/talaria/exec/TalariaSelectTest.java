/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.framework.TalariaTestRunner;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TalariaSelectTest extends TalariaTestRunner
{
  @Test
  public void testSelectOnFoo()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo")
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE1)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("cnt", "dim1")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .columnMappings(ColumnMappings.identity(resultSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, !useDefault ? "" : null},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        )).verifyResults();

  }


  @Test
  public void testSelectOnFoo2()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE2)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("dim2", "m1")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .columnMappings(ColumnMappings.identity(resultSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        )).verifyResults();
  }

  @Test
  public void testGroupByOnFoo()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(GroupByQuery.builder()
                                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                                        .setInterval(querySegmentSpec(Filtration
                                                                                                             .eternity()))
                                                                        .setGranularity(Granularities.ALL)
                                                                        .setDimensions(dimensions(
                                                                               new DefaultDimensionSpec(
                                                                                   "cnt",
                                                                                   "d0",
                                                                                   ColumnType.LONG
                                                                               )
                                                                           ))
                                                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                                               "a0")))
                                                                        .setContext(DEFAULT_TALARIA_CONTEXT)
                                                                        .build())
                                                     .columnMappings(ColumnMappings.identity(rowSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{1L, 6L}
            )).verifyResults();
  }

  @Test
  public void testGroupByOrderByDimension()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "d0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            null
                        )
                    )
                    .setContext(DEFAULT_TALARIA_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, count(*) as cnt from foo group by m1 order by m1 desc")
        .setExpectedTalariaQuerySpec(
            TalariaQuerySpec.builder()
                            .query(query)
                            .columnMappings(ColumnMappings.identity(rowSignature))
                            .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                            .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 1L},
                new Object[]{5f, 1L},
                new Object[]{4f, 1L},
                new Object[]{3f, 1L},
                new Object[]{2f, 1L},
                new Object[]{1f, 1L}
            )
        )
        .verifyResults();
  }

  @Test
  public void testGroupByOrderByAggregation()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            null
                        )
                    )
                    .setContext(DEFAULT_TALARIA_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc")
        .setExpectedTalariaQuerySpec(
            TalariaQuerySpec.builder()
                            .query(query)
                            .columnMappings(ColumnMappings.identity(rowSignature))
                            .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                            .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 6d},
                new Object[]{5f, 5d},
                new Object[]{4f, 4d},
                new Object[]{3f, 3d},
                new Object[]{2f, 2d},
                new Object[]{1f, 1d}
            )
        ).verifyResults();
  }

  @Test
  public void testExternSelect1() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadAsJson = queryJsonMapper.writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedTalariaQuerySpec(
                         TalariaQuerySpec
                             .builder()
                             .query(GroupByQuery.builder()
                                                .setDataSource(new ExternalDataSource(
                                                       new LocalInputSource(
                                                           null,
                                                           null,
                                                           ImmutableList.of(
                                                               toRead.getAbsoluteFile()
                                                           )
                                                       ),
                                                       new JsonInputFormat(null, null, null),
                                                       RowSignature.builder()
                                                                   .add("timestamp", ColumnType.STRING)
                                                                   .add("page", ColumnType.STRING)
                                                                   .add("user", ColumnType.STRING)
                                                                   .build()
                                                   ))
                                                .setInterval(querySegmentSpec(
                                                       Filtration
                                                           .eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                       "v0",
                                                       "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'P1D',null,'UTC')",
                                                       ColumnType.LONG,
                                                       CalciteTests.createExprMacroTable()
                                                   ))
                                                .setDimensions(dimensions(new DefaultDimensionSpec(
                                                                                 "v0",
                                                                                 "d0",
                                                                                 ColumnType.LONG
                                                                             )
                                                   ))
                                                .setAggregatorSpecs(aggregators(
                                                       new CountAggregatorFactory(
                                                           "a0")))
                                                .setContext(
                                                       ImmutableMap.of())
                                                .build())
                             .columnMappings(ColumnMappings.identity(
                                 rowSignature))
                             .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                             .build())
                     .verifyResults();
  }

  @Test
  public void testIncorrectSelectQuery()
  {
    testSelectQuery()
        .setSql("select a from ")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(SqlPlanningException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered \"from <EOF>\""))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnInformationSchemaSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Cannot read SQL metadata tables with Talaria enabled"))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM sys.segments")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Cannot read SQL metadata tables with Talaria enabled"))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceWithJoin()
  {
    testSelectQuery()
        .setSql("select s.segment_id, s.num_rows, f.dim1 from sys.segments as s, foo as f")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Cannot read SQL metadata tables with Talaria enabled"))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceContainingWith()
  {
    testSelectQuery()
        .setSql("with segment_source as (SELECT * FROM sys.segments) "
                + "select segment_source.segment_id, segment_source.num_rows from segment_source")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Cannot read SQL metadata tables with Talaria enabled"))
        ))
        .verifyPlanningErrors();
  }


  @Test
  public void testSelectOnUserDefinedSourceContainingWith()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("with sys as (SELECT * FROM foo2) "
                + "select m1, dim2 from sys")
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE2)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("dim2", "m1")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .columnMappings(ColumnMappings.identity(resultSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        ))
        .verifyResults();
  }

  @Test
  public void testScanWithMultiValueSelectQuery()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim3", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select dim3 from foo")
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE1)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("dim3")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .columnMappings(ColumnMappings.identity(resultSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{ImmutableList.of("a", "b")},
            new Object[]{ImmutableList.of("b", "c")},
            new Object[]{"d"},
            new Object[]{!useDefault ? "" : null},
            new Object[]{null},
            new Object[]{null}
        )).verifyResults();
  }

  @Test
  public void testGroupByWithMultiValuePivotSelectQuery()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_TALARIA_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", true)
                                              .build();
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim3", ColumnType.STRING)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(GroupByQuery.builder()
                                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                                        .setGranularity(Granularities.ALL)
                                                                        .setDimensions(
                                                                               dimensions(
                                                                                   new DefaultDimensionSpec(
                                                                                       "dim3",
                                                                                       "d0"
                                                                                   )
                                                                               )
                                                                           )
                                                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                                               "a0")))
                                                                        .setContext(context)
                                                                        .build()
                                                     )
                                                     .columnMappings(ColumnMappings.identity(rowSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroup()
        )
        .verifyResults();
  }


  @Test
  public void testGroupByWithMultiValuePivotSelectQueryWithoutGroupByEnable()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_TALARIA_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", false)
                                              .build();

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false."))
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByWithMultiValueMvToArrayPivotSelectQuery()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_TALARIA_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", true)
                                              .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(GroupByQuery.builder()
                                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                                        .setGranularity(Granularities.ALL)
                                                                        .setDimensions(
                                                                               dimensions(
                                                                                   new DefaultDimensionSpec(
                                                                                       "dim3",
                                                                                       "d0"
                                                                                   )
                                                                               )
                                                                           )
                                                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                                               "a0")))
                                                                        .setPostAggregatorSpecs(
                                                                               ImmutableList.of(new ExpressionPostAggregator(
                                                                                   "p0",
                                                                                   "mv_to_array(\"d0\")",
                                                                                   null, ExprMacroTable.nil())
                                                                               )
                                                                           )
                                                                        .setContext(context)
                                                                        .build()
                                                     )
                                                     .columnMappings(ColumnMappings.identity(rowSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroupByList()
        )
        .verifyResults();
  }

  @Test
  public void testGroupByWithMultiValueMvToArrayPivotSelectQueryWithoutGroupByEnable()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_TALARIA_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", false)
                                              .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false."))
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByMultiValueMeasureQuery()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select __time, count(dim3) as cnt1 from foo group by __time")
        .setQueryContext(DEFAULT_TALARIA_CONTEXT)
        .setExpectedTalariaQuerySpec(TalariaQuerySpec.builder()
                                                     .query(GroupByQuery.builder()
                                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                                        .setGranularity(Granularities.ALL)
                                                                        .setDimensions(
                                                                               dimensions(
                                                                                   new DefaultDimensionSpec(
                                                                                       "__time",
                                                                                       "d0",
                                                                                       ColumnType.LONG
                                                                                   )
                                                                               )
                                                                           )
                                                                        .setAggregatorSpecs(
                                                                               aggregators(
                                                                                   new FilteredAggregatorFactory(
                                                                                       new CountAggregatorFactory("a0"),
                                                                                       new NotDimFilter(new SelectorDimFilter("dim3", null, null)),
                                                                                       "a0"
                                                                                   )))
                                                                        .setContext(DEFAULT_TALARIA_CONTEXT)
                                                                        .build()
                                                     )
                                                     .columnMappings(ColumnMappings.identity(rowSignature))
                                                     .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{946684800000L, 1L},
                new Object[]{946771200000L, 1L},
                new Object[]{946857600000L, 1L},
                new Object[]{978307200000L, !useDefault ? 1L : 0L},
                new Object[]{978393600000L, 0L},
                new Object[]{978480000000L, 0L}
            )
        )
        .verifyResults();
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroup()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{null, !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{"", 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{"a", 1L},
        new Object[]{"b", 2L},
        new Object[]{"c", 1L},
        new Object[]{"d", 1L}
    ));
    return expected;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroupByList()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{Collections.singletonList(null), !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{Collections.singletonList(""), 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{Collections.singletonList("a"), 1L},
        new Object[]{Collections.singletonList("b"), 2L},
        new Object[]{Collections.singletonList("c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    ));
    return expected;
  }
}
