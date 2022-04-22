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
import io.imply.druid.talaria.sql.TalariaQueryMaker;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TalariaInsertTest extends TalariaTestRunner
{

  @Test
  public void testInsertOnFoo1()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

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

    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1Range()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();
    Map<String, Object> queryContext = new HashMap<>(DEFAULT_TALARIA_CONTEXT);
    queryContext.put(TalariaQueryMaker.CTX_REPLACE_TIME_CHUNKS, "ALL");

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

    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setDataSource("foo1")
                     .setQueryContext(queryContext)
                     .setShardSpecClass(DimensionRangeShardSpec.class)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();

  }

  @Test
  public void testIncorrectInsertQuery()
  {
    testInsertQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 clustered by dim1")
                     .setValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                             "INSERT statements must specify PARTITIONED BY clause explicitly"))
                     ))
                     .verifyPlanningErrors();


  }
}
