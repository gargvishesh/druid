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
import io.imply.druid.talaria.framework.TalariaTestRunner;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

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
        .setTalariaQuerySpec(TalariaQuerySpec.builder()
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
        .setTalariaQuerySpec(TalariaQuerySpec.builder()
                                             .setQuery(newScanQueryBuilder()
                                                           .dataSource(CalciteTests.DATASOURCE1)
                                                           .intervals(querySegmentSpec(Filtration.eternity()))
                                                           .columns("m1", "dim2")
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
        .setTalariaQuerySpec(TalariaQuerySpec.builder()
                                             .setQuery(GroupByQuery.builder()
                                                                   .setDataSource(CalciteTests.DATASOURCE1)
                                                                   .setInterval(querySegmentSpec(Filtration
                                                                                                     .eternity()))
                                                                   .setGranularity(Granularities.ALL)
                                                                   .setDimensions(dimensions(
                                                                       new DefaultDimensionSpec(
                                                                           "cnt",
                                                                           "cnt"
                                                                       )
                                                                   ))
                                                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                                       "cnt1")))
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
  public void testIncorrectSelectQuery()
  {
    testSelectQuery()
        .setSql("select a from ")
        .setValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(SqlPlanningException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered \"from <EOF>\""))
        ))
        .verifyPlanningErrors();
  }
}
