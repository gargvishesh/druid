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
import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.framework.TalariaTestRunner;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import io.imply.druid.talaria.sql.TalariaMode;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * unparseable.gz is a file containing 10 valid and 9 invalid records
 */
public class TalariaWarningsTest extends TalariaTestRunner
{

  private File toRead;
  private RowSignature rowSignature;
  private TalariaQuerySpec talariaQuerySpec;
  private String toReadFileNameAsJson;

  @Before
  public void setUp3() throws IOException
  {
    toRead = getResourceAsTemporaryFile("/unparseable.gz");
    toReadFileNameAsJson = queryJsonMapper.writeValueAsString(toRead.getAbsolutePath());

    rowSignature = RowSignature.builder()
                               .add("__time", ColumnType.LONG)
                               .add("cnt", ColumnType.LONG)
                               .build();

    talariaQuerySpec = TalariaQuerySpec.builder()
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
                           .setInterval(querySegmentSpec(Filtration.eternity()))
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
                           .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                           .setContext(ImmutableMap.of())
                           .build())
        .columnMappings(ColumnMappings.identity(rowSignature))
        .tuningConfig(ParallelIndexTuningConfig.defaultConfig())
        .build();
  }


  @Test
  public void testThrowExceptionWhenParseExceptionsExceedLimit()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }

  @Test
  public void testSuccessWhenNoLimitEnforced()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .verifyResults();
  }

  @Test
  public void testInvalidMaxParseExceptionsPassed()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -2,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .setExpectedTalariaFault(UnknownFault.forMessage(
                         "java.lang.IllegalArgumentException: "
                         + "Invalid limit of -2 supplied for warnings of type CannotParseExternalData. "
                         + "Limit can be greater than or equal to -1."))
                     .verifyResults();
  }

  @Test
  public void testFailureWhenParseExceptionsExceedPositiveLimit()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 4,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .setExpectedTalariaFault(new TooManyWarningsFault(4, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }


  @Test
  public void testSuccessWhenParseExceptionsOnLimit()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .verifyResults();
  }

  @Test
  public void testSuccessInNonStrictMode()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaMode.CTX_TALARIA_MODE, "nonStrict",
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .verifyResults();
  }


  @Test
  public void testFailureInStrictMode()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0,
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }

  @Test
  public void testDefaultStrictMode()
  {
    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }

  @Test
  public void testLeaderTemporaryFileCleanup()
  {
    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();

    // Temporary directory should not contain any leader folders
    Assert.assertEquals(0, localFileStorageDir.listFiles().length);
  }

  @Test
  public void testSuccessWhenModeIsOverridden()
  {
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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1,
                         TalariaMode.CTX_TALARIA_MODE, "strict",
                         "multiStageQuery", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedTalariaQuerySpec(talariaQuerySpec)
                     .verifyResults();
  }
}
