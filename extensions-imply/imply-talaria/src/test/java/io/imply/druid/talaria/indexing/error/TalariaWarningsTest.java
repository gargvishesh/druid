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

public class TalariaWarningsTest extends TalariaTestRunner
{

  @Test
  public void testThrowExceptionWhenParseExceptionsExceedLimit()
  {
    File toRead = new File("src/test/resources/unparseable.gz");

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
                     .setQueryContext(ImmutableMap.of(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0, "talaria", true))
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
  public void testSuccessWhenParseExceptionsBelowLimit()
  {
    File toRead = new File("src/test/resources/unparseable.gz");

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
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 202862L}))
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
