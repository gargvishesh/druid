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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * unparseable.gz is a file containing 10 valid and 9 invalid records
 * <p>
 * TODO: There is redundancy in the code for the test cases, because the toRead parameter in them is only extracted out
 *  in the methods. This can be looked into by using static methods to populate the required fields
 */
public class TalariaWarningsTest extends TalariaTestRunner
{

  @Test
  public void testThrowExceptionWhenParseExceptionsExceedLimit() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0,
                         "talaria", true
                     ))
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
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }

  @Test
  public void testSuccessWhenNoLimitEnforced() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1,
                         "talaria", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
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
  public void testInvalidMaxParseExceptionsPassed() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -2,
                         "talaria", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
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
                     .setExpectedTalariaFault(UnknownFault.forMessage(
                         "java.lang.IllegalArgumentException: "
                         + "Invalid limit of -2 supplied for warnings of type CannotParseExternalData. "
                         + "Limit can be greater than or equal to -1."))
                     .verifyResults();
  }

  @Test
  public void testFailureWhenParseExceptionsExceedPositiveLimit() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 4,
                         "talaria", true
                     ))
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
                     .setExpectedTalariaFault(new TooManyWarningsFault(4, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }


  @Test
  public void testSuccessWhenParseExceptionsOnLimit() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10,
                         "talaria", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
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
  public void testSuccessInNonStrictMode() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaMode.CTX_TALARIA_MODE, "nonStrict",
                         "talaria", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
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
  public void testFailureInStrictMode() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0,
                         "talaria", true
                     ))
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
                     .setExpectedTalariaFault(new TooManyWarningsFault(0, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }

  @Test
  public void testDefaultStrictMode() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled-unparsable.json");
    final String toReadFileNameAsJson = queryJsonMapper.writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

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
  public void testSuccessWhenModeIsOverridden() throws IOException
  {
    File toRead = getResourceAsTemporaryFile("/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(
                         TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1,
                         TalariaMode.CTX_TALARIA_MODE, "strict",
                         "talaria", true
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
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
}
