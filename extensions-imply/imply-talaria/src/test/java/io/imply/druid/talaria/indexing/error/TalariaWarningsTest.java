/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.collect.ImmutableList;
import io.imply.druid.talaria.framework.TalariaTestRunner;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TalariaWarningsTest extends TalariaTestRunner
{

  @Test
  public void testThrowExceptionWhenParseExceptionsExceedLimit()
  {
    File toRead = new File("src/test/resources/unparseable.gz");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .add("a0", ColumnType.LONG)
                                            .build();

    Map<String, Object> context = new HashMap<>(ROLLUP_CONTEXT);
    context.put("maxParseExceptions", 0);
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
                     .setQueryContext(context)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L, 0L}))
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();

  }

  @Test
  public void testSuccessWhenParseExceptionsBelowLimit()
  {
    File toRead = new File("src/test/resources/unparseable.gz");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .add("a0", ColumnType.LONG)
                                            .build();

    Map<String, Object> context = new HashMap<>(ROLLUP_CONTEXT);
    context.put("maxParseExceptions", -1);
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
                     .setQueryContext(context)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("a0", "a0"))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L, 0L}))
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();

  }

}
