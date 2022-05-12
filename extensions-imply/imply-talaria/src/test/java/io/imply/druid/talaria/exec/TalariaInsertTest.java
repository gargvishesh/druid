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
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TalariaInsertTest extends TalariaTestRunner
{

  @Test
  public void testInsertOnFoo1()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testInsertOnExternalDataSource()
  {
    File toRead = new File("src/test/resources/wikipedia-sampled.json");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testInsertQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithTimeFunction()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testInsertQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  @Ignore()
  public void testRollUpOnFoo1()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("a0", ColumnType.LONG).build();

    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  @Ignore()
  public void testRollUpOnFoo1WithTimeFunction()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("a0", ColumnType.LONG).build();


    testInsertQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRowsWithAggregatedComplexColumn())
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  @Ignore()
  public void testRollUpOnFoo1WithTimeFunctionComplexCol()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("a0", new ColumnType(ValueType.COMPLEX, "hyperUnique", null))
                                            .build();


    testInsertQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(distinct m1) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new HyperUniquesAggregatorFactory("a0", "a0", false, true))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRowsWithAggregatedComplexColumn())
                     .verifyResults();

  }


  @Test
  @Ignore()
  public void testRollUpOnFoo1ComplexCol()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("a0", new ColumnType(ValueType.COMPLEX, "hyperUnique", null))
                                            .build();


    testInsertQuery().setSql(
                         "insert into foo1 select  __time , dim1 , count(distinct m1) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new HyperUniquesAggregatorFactory("a0", "a0", false, true))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRowsWithAggregatedComplexColumn())
                     .verifyResults();

  }

  @Test
  @Ignore()
  public void testRollUpOnExternalDataSource()
  {
    File toRead = new File("src/test/resources/wikipedia-sampled.json");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("a0", ColumnType.LONG)
                                            .build();

    testInsertQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .verifyResults();
  }

  @Test()
  @Ignore()
  public void testRollUpOnExternalDataSourceWithCompositeKey()
  {
    File toRead = new File("src/test/resources/wikipedia-sampled.json");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("namespace", ColumnType.STRING)
                                            .add("a0", ColumnType.LONG)
                                            .build();

    testInsertQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + " namespace , count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1,2  PARTITIONED by day ")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{1466985600000L, "Benutzer Diskussion", 2L},
                         new Object[]{1466985600000L, "File", 1L},
                         new Object[]{1466985600000L, "Kategoria", 1L},
                         new Object[]{1466985600000L, "Main", 14L},
                         new Object[]{1466985600000L, "Wikipedia", 1L},
                         new Object[]{1466985600000L, "Википедия", 1L}
                     ))
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1Range()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(REPLACE_TIME_CHUCKS_CONTEXT)
                     .setExpectedShardSpec(DimensionRangeShardSpec.class)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testIncorrectInsertQuery()
  {
    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 clustered by dim1")
                     .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                             "INSERT statements must specify PARTITIONED BY clause explicitly"))
                     ))
                     .verifyPlanningErrors();


  }


  @Nonnull
  private List<Object[]> expectedFooRows()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    if (!useDefault) {
      expectedRows.add(new Object[]{946684800000L, "", 1L});
    }
    expectedRows.addAll(ImmutableList.of(
        new Object[]{946771200000L, "10.1", 1L},
        new Object[]{946857600000L, "2", 1L},
        new Object[]{978307200000L, "1", 1L},
        new Object[]{978393600000L, "def", 1L},
        new Object[]{978480000000L, "abc", 1L}
    ));
    return expectedRows;
  }

  @Nonnull
  private List<Object[]> expectedFooRowsWithAggregatedComplexColumn()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    HyperLogLogCollector hyperLogLogCollector = HyperLogLogCollector.makeLatestCollector();
    if (!useDefault) {
      expectedRows.add(new Object[]{946684800000L, "", hyperLogLogCollector.estimateCardinality()});
    }
    expectedRows.addAll(ImmutableList.of(
        new Object[]{946771200000L, "10.1", hyperLogLogCollector.estimateCardinality()},
        new Object[]{946857600000L, "2", hyperLogLogCollector.estimateCardinality()},
        new Object[]{978307200000L, "1", hyperLogLogCollector.estimateCardinality()},
        new Object[]{978393600000L, "def", hyperLogLogCollector.estimateCardinality()},
        new Object[]{978480000000L, "abc", hyperLogLogCollector.estimateCardinality()}
    ));
    return expectedRows;
  }
}
