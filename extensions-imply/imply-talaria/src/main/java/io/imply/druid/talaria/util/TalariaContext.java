/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import java.util.Map;

/**
 * Class for all talaria context params
 */
public class TalariaContext
{
  public static final String CTX_MAX_NUM_CONCURRENT_SUB_TASKS = "talariaNumTasks";
  public static final String CTX_FINALIZE_AGGREGATIONS = "talariaFinalizeAggregations";

  public static final String CTX_DURABLE_SHUFFLE_STORAGE = "talariaDurableShuffleStorage";

  public static final String CTX_DESTINATION = "talariaDestination";
  public static final String CTX_ROWS_PER_SEGMENT = "talariaRowsPerSegment";
  public static final String CTX_ROWS_IN_MEMORY = "talariaRowsInMemory";

  public static boolean isDurableStorageEnabled(Map<String, Object> propertyMap)
  {
    return Boolean.parseBoolean(
        String.valueOf(propertyMap.getOrDefault(CTX_DURABLE_SHUFFLE_STORAGE, "false")));
  }
}
