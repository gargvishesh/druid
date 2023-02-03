package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
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
            new Object[]{"[946684800000,946684810000,946684820000]"},
            new Object[]{"[946771200000,946771210000,946771220000]"},
            new Object[]{"[946857600000,946857610000,946857620000]"}
        )
    );
  }
}