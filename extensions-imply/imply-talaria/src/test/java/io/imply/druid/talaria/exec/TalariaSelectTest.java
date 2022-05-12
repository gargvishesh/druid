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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
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

import java.io.File;

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
                                                     .setQuery(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE1)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("cnt", "dim1")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .setColumnMappings(ColumnMappings.identity(resultSignature))
                                                     .setTuningConfig(ParallelIndexTuningConfig.defaultConfig())
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
                                                     .setQuery(newScanQueryBuilder()
                                                                   .dataSource(CalciteTests.DATASOURCE2)
                                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                                   .columns("dim2", "m1")
                                                                   .context(DEFAULT_TALARIA_CONTEXT)
                                                                   .build())
                                                     .setColumnMappings(ColumnMappings.identity(resultSignature))
                                                     .setTuningConfig(ParallelIndexTuningConfig.defaultConfig())
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
                                                     .setQuery(GroupByQuery.builder()
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
                                                     .setColumnMappings(ColumnMappings.identity(rowSignature))
                                                     .setTuningConfig(ParallelIndexTuningConfig.defaultConfig())
                                                     .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{1L, 6L}
            )).verifyResults();
  }

  @Test
  public void testExternSelect1()
  {
    File toRead = new File("src/test/resources/wikipedia-sampled.json");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedTalariaQuerySpec(
                         TalariaQuerySpec
                             .builder()
                             .setQuery(GroupByQuery.builder()
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
                             .setColumnMappings(ColumnMappings.identity(
                                 rowSignature))
                             .setTuningConfig(ParallelIndexTuningConfig.defaultConfig())
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
}
