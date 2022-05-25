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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlPlanningException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.File;
import java.io.IOException;

public class TalariaReplaceTest extends TalariaTestRunner
{
  @Test
  public void testReplaceOnFooWithAll()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f},
                             new Object[]{946857600000L, 3.0f},
                             new Object[]{978307200000L, 4.0f},
                             new Object[]{978393600000L, 5.0f},
                             new Object[]{978480000000L, 6.0f}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFooWithWhere()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE WHERE __time >= TIMESTAMP '2000-01-02' AND __time < TIMESTAMP '2000-01-03' "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-02' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED by DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of("2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.000Z")))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{946771200000L, 2.0f}))
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFoo1WithAllExtern() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG).build();

    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryJsonMapper.writeValueAsString(toRead.getAbsolutePath());

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE ALL SELECT "
                             + "  floor(TIME_PARSE(\"timestamp\") to hour) AS __time, "
                             + "  count(*) AS cnt "
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") GROUP BY 1  PARTITIONED BY HOUR ")
                     .setExpectedDataSource("foo1")
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{1466985600000L, 10L},
                             new Object[]{1466989200000L, 4L},
                             new Object[]{1466992800000L, 6L}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFoo1WithWhereExtern() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("user", ColumnType.STRING).build();

    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryJsonMapper.writeValueAsString(toRead.getAbsolutePath());

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE WHERE __time >= TIMESTAMP '2016-06-27 01:00:00.00' AND __time < TIMESTAMP '2016-06-27 02:00:00.00' "
                             + " SELECT "
                             + "  floor(TIME_PARSE(\"timestamp\") to hour) AS __time, "
                             + "  user "
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") "
                             + "where \"timestamp\" >= TIMESTAMP '2016-06-27 01:00:00.00' AND \"timestamp\" < TIMESTAMP '2016-06-27 02:00:00.00' "
                             + "PARTITIONED BY HOUR ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of("2016-06-27T01:00:00.000Z/2016-06-27T02:00:00.000Z")))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{1466989200000L, "2001:DA8:207:E132:94DC:BA03:DFDF:8F9F"},
                             new Object[]{1466989200000L, "Ftihikam"},
                             new Object[]{1466989200000L, "Guly600"},
                             new Object[]{1466989200000L, "Kolega2357"}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceIncorrectSyntax()
  {
    testIngestQuery().setSql("REPLACE INTO foo1 OVERWRITE SELECT * FROM foo PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedValidationErrorMatcher(
                         CoreMatchers.allOf(
                             CoreMatchers.instanceOf(SqlPlanningException.class),
                             ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                                 "Missing time chunk information in OVERWRITE clause for REPLACE, set it to OVERWRITE WHERE <__time based condition> or set it to overwrite the entire table with OVERWRITE ALL."))
                         )
                     )
                     .verifyPlanningErrors();
  }
}
