/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.QueryContext;

import java.util.Map;

/**
 * Class for all msq context params
 */
public class TalariaContext
{
  public static final String CTX_MAX_NUM_TASKS = "msqMaxNumTasks";
  public static final String CTX_MAX_NUM_TASKS_OLD = "msqNumTasks";
  public static final String CTX_TASK_ASSIGNMENT = "msqTaskAssignment";
  public static final String CTX_FINALIZE_AGGREGATIONS = "msqFinalizeAggregations";

  public static final String CTX_DURABLE_SHUFFLE_STORAGE = "msqDurableShuffleStorage";

  public static final String CTX_DESTINATION = "msqDestination";
  public static final String CTX_ROWS_PER_SEGMENT = "msqRowsPerSegment";
  public static final String CTX_ROWS_IN_MEMORY = "msqRowsInMemory";

  private static final int DEFAULT_MAX_NUM_TASKS = 2;

  public static boolean isDurableStorageEnabled(Map<String, Object> propertyMap)
  {
    return Boolean.parseBoolean(
        String.valueOf(propertyMap.getOrDefault(CTX_DURABLE_SHUFFLE_STORAGE, "false")));
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return Numbers.parseBoolean(queryContext.getOrDefault(TalariaContext.CTX_FINALIZE_AGGREGATIONS, true));
  }

  public static WorkerAssignmentStrategy getAssignmentStrategy(final QueryContext queryContext)
  {
    final String assignmentStrategyString = queryContext.getAsString(CTX_TASK_ASSIGNMENT);

    if (assignmentStrategyString == null) {
      return WorkerAssignmentStrategy.MAX;
    } else {
      return WorkerAssignmentStrategy.fromString(assignmentStrategyString);
    }
  }

  public static int getMaxNumTasks(final QueryContext queryContext)
  {
    return queryContext.getAsInt(
        CTX_MAX_NUM_TASKS,
        queryContext.getAsInt(
            CTX_MAX_NUM_TASKS_OLD,
            DEFAULT_MAX_NUM_TASKS
        )
    );
  }
}
