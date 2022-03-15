/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.math.IntMath;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyInputFilesFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.StageDefinition;
import org.apache.druid.java.util.common.ISE;

import java.math.RoundingMode;

public class QueryDefinitionValidator
{
  /**
   * Checks {@link QueryDefinition} if its valid else throws an exception
   *
   * @param queryDef query definition to validate
   */
  public static void validateQueryDef(final QueryDefinition queryDef)
  {
    for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
      final int numColumns = stageDef.getSignature().size();

      if (numColumns > Limits.MAX_FRAME_COLUMNS) {
        throw new TalariaException(new TooManyColumnsFault(numColumns, Limits.MAX_FRAME_COLUMNS));
      }

      final int numWorkers = stageDef.getMaxWorkerCount();
      if (numWorkers > Limits.MAX_WORKERS) {
        throw new TalariaException(new TooManyWorkersFault(numWorkers, Limits.MAX_WORKERS));
      } else if (numWorkers <= 0) {
        throw new ISE("Number of workers should be greater than 0");
      }

      final int numInputFiles = stageDef.getProcessorFactory().inputFileCount();

      if (IntMath.divide(numInputFiles, numWorkers, RoundingMode.CEILING) > Limits.MAX_INPUT_FILES_PER_WORKER) {
        throw new TalariaException(new TooManyInputFilesFault(
            numInputFiles,
            Limits.MAX_INPUT_FILES_PER_WORKER,
            IntMath.divide(numInputFiles, Limits.MAX_INPUT_FILES_PER_WORKER, RoundingMode.CEILING)
        ));
      }
    }
  }
}
