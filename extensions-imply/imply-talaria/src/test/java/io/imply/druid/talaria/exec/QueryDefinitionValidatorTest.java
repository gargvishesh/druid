/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageDefinitionBuilder;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.stream.IntStream;

public class QueryDefinitionValidatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidQueryDefination()
  {
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(1, 1, 1));
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(
        Limits.MAX_FRAME_COLUMNS,
        Limits.MAX_WORKERS,
        Limits.MAX_WORKERS
        * Limits.MAX_INPUT_FILES_PER_WORKER
    ));
  }

  @Test
  public void testNegativeWorkers()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Number of workers should be greater than 0");
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(1, -1, 1));
  }

  @Test
  public void testZeroWorkers()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Number of workers should be greater than 0");
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(1, 0, 1));
  }

  @Test
  public void testGreaterThanMaxWorkers()
  {
    expectedException.expect(TalariaException.class);
    expectedException.expectMessage(
        StringUtils.format(
            "Too many workers (current = %d; max = %d)",
            Limits.MAX_WORKERS + 1,
            Limits.MAX_WORKERS
        ));
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(1, Limits.MAX_WORKERS + 1, 1));
  }

  @Test
  public void testGreaterThanMaxColumns()
  {
    expectedException.expect(TalariaException.class);
    expectedException.expectMessage(StringUtils.format(
        "Too many output columns (requested = %d, max = %d)",
        Limits.MAX_FRAME_COLUMNS + 1,
        Limits.MAX_FRAME_COLUMNS
    ));
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(Limits.MAX_FRAME_COLUMNS + 1, 1, 1));
  }

  @Test
  public void testMoreInputFiles()
  {
    int numWorkers = 3;
    int inputFiles = numWorkers * Limits.MAX_INPUT_FILES_PER_WORKER + 1;
    expectedException.expect(TalariaException.class);
    expectedException.expectMessage(StringUtils.format(
        "Too many input files/segments [%d] encountered. Maximum input files/segments per worker is set to [%d]. Try"
        + " breaking your query up into smaller queries, or increasing the number of workers to at least [%d] by"
        + " setting %s in your query context",
        inputFiles,
        Limits.MAX_INPUT_FILES_PER_WORKER,
        numWorkers + 1,
        TalariaContext.CTX_MAX_NUM_CONCURRENT_SUB_TASKS
    ));
    QueryDefinitionValidator.validateQueryDef(createQueryDefinition(1, numWorkers, inputFiles));
  }

  private static QueryDefinition createQueryDefinition(int numColumns, int numWorkers, int numInputFiles)
  {
    QueryDefinitionBuilder builder = QueryDefinition.builder();
    builder.queryId(UUID.randomUUID().toString());

    StageDefinitionBuilder stageBuilder = StageDefinition.builder(0);
    builder.add(stageBuilder);
    stageBuilder.maxWorkerCount(numWorkers);

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    IntStream.range(0, numColumns).forEach(col -> rowSignatureBuilder.add("col_" + col, ColumnType.STRING));
    stageBuilder.signature(rowSignatureBuilder.build());

    stageBuilder.processorFactory(new TestFrameProcessorFactory(numInputFiles));
    return builder.build();
  }
}
