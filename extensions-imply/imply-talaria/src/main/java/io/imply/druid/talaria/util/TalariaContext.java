/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.QueryContext;

import java.util.Map;

/**
 * Class for all msq context params
 */
public class TalariaContext
{
  public static final String CTX_MAX_NUM_CONCURRENT_SUB_TASKS = "msqNumTasks";
  public static final String AUTO_TASK_COUNT_MODE = "auto";
  public static final Integer UNKOWN_TASK_COUNT = Integer.MAX_VALUE;
  public static final String CTX_FINALIZE_AGGREGATIONS = "msqFinalizeAggregations";

  public static final String CTX_DURABLE_SHUFFLE_STORAGE = "msqDurableShuffleStorage";

  public static final String CTX_DESTINATION = "msqDestination";
  public static final String CTX_ROWS_PER_SEGMENT = "msqRowsPerSegment";
  public static final String CTX_ROWS_IN_MEMORY = "msqRowsInMemory";

  public static boolean isDurableStorageEnabled(Map<String, Object> propertyMap)
  {
    return Boolean.parseBoolean(
        String.valueOf(propertyMap.getOrDefault(CTX_DURABLE_SHUFFLE_STORAGE, "false")));
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return Numbers.parseBoolean(queryContext.getOrDefault(TalariaContext.CTX_FINALIZE_AGGREGATIONS, true));
  }

  public static boolean isTaskCountUnknown(int numTasks)
  {
    return numTasks == UNKOWN_TASK_COUNT;
  }

  public static boolean isTaskAutoModeEnabled(final QueryContext queryContext)
  {
    String mode = queryContext.getAsString(TalariaContext.CTX_MAX_NUM_CONCURRENT_SUB_TASKS);
    return mode != null && mode.equalsIgnoreCase(AUTO_TASK_COUNT_MODE);
  }
}
