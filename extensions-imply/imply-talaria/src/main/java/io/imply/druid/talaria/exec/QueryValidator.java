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
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyInputFilesFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.WorkOrder;
import org.apache.druid.java.util.common.ISE;

import java.math.RoundingMode;

public class QueryValidator
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
        throw new ISE("Number of workers must be greater than 0");
      }
    }
  }

  public static void validateWorkOrder(final WorkOrder order)
  {
    final int numInputFiles = Ints.checkedCast(order.getInputs().stream().mapToLong(InputSlice::numFiles).sum());

    if (numInputFiles > Limits.MAX_INPUT_FILES_PER_WORKER) {
      throw new TalariaException(
          new TooManyInputFilesFault(
              numInputFiles,
              Limits.MAX_INPUT_FILES_PER_WORKER,
              IntMath.divide(numInputFiles, Limits.MAX_INPUT_FILES_PER_WORKER, RoundingMode.CEILING)
          )
      );
    }
  }
}
