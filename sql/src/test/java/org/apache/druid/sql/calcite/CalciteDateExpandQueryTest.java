/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class CalciteDateExpandQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testDateExpandOperator()
  {
    testQuery(
        "select DATE_EXPAND(TIMESTAMP_TO_MILLIS(__time), TIMESTAMP_TO_MILLIS(__time) + 20000, 'PT10S') "
        + "FROM foo "
        + "WHERE __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999' ",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .virtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "date_expand(\"__time\",(\"__time\" + 20000),'PT10S')",
                                ColumnType.LONG_ARRAY
                            )
                        )
                  .columns("v0")
                  .legacy(false)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"[946684800000,946684810000]"},
            new Object[]{"[946771200000,946771210000]"},
            new Object[]{"[946857600000,946857610000]"}
        )
    );
  }

  @Test
  public void testDateExpandOperatorWithInvalidRange()
  {
    testQuery(
        "select DATE_EXPAND(TIMESTAMP_TO_MILLIS(__time), TIMESTAMP_TO_MILLIS(__time) - 1, 'PT10S') "
            + "FROM foo "
            + "WHERE __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999' ",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                .virtualColumns(
                    expressionVirtualColumn(
                        "v0",
                        "date_expand(\"__time\",(\"__time\" - 1),'PT10S')",
                        ColumnType.LONG_ARRAY
                    )
                )
                .columns("v0")
                .legacy(false)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[]"},
            new Object[]{"[]"},
            new Object[]{"[]"}
        )
    );
  }

  @Test
  public void testDateExpandOperatorWithUnnest()
  {
    cannotVectorize();
    skipVectorize();
    testQuery(
        "WITH X as \n"
        + "(\n"
        + "SELECT\n"
        + "DATE_EXPAND(TIMESTAMP_TO_MILLIS(TIME_PARSE('2023-12-04 06:00:00')), TIMESTAMP_TO_MILLIS(TIME_PARSE('2023-12-05 06:00:00')), 'PT1H' ) as alldates\n"
        + "FROM foo\n"
        + "GROUP BY 1\n"
        + ")\n"
        + "select * from X CROSS JOIN UNNEST(X.alldates) as ud(singleDate) ",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                                  new QueryDataSource(
                                      GroupByQuery.builder()
                                                  .setDataSource(CalciteTests.DATASOURCE1)
                                                  .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                                      Filtration.eternity())))
                                                  .setVirtualColumns(expressionVirtualColumn(
                                                      "v0",
                                                      "array(1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000)",
                                                      ColumnType.LONG_ARRAY
                                                  ))
                                                  .setDimensions(dimensions(
                                                      new DefaultDimensionSpec(
                                                          "v0",
                                                          "d0",
                                                          ColumnType.LONG_ARRAY
                                                      )
                                                  ))
                                                  .setGranularity(Granularities.ALL)
                                                  .setContext(QUERY_CONTEXT_DEFAULT)
                                                  .build()),
                                  expressionVirtualColumn(
                                      "j0.unnest",
                                      "array(1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000)",
                                      ColumnType.LONG_ARRAY
                                  ),
                                  null
                              )
                  )
                  .eternityInterval()
                  .columns("d0", "j0.unnest")
                  .legacy(false)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701669600000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701673200000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701676800000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701680400000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701684000000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701687600000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701691200000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701694800000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701698400000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701702000000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701705600000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701709200000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701712800000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701716400000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701720000000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701723600000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701727200000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701730800000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701734400000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701738000000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701741600000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701745200000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701748800000L},
            new Object[]{"[1701669600000,1701673200000,1701676800000,1701680400000,1701684000000,1701687600000,1701691200000,1701694800000,1701698400000,1701702000000,1701705600000,1701709200000,1701712800000,1701716400000,1701720000000,1701723600000,1701727200000,1701730800000,1701734400000,1701738000000,1701741600000,1701745200000,1701748800000,1701752400000]", 1701752400000L}
        )
    );
  }
}
