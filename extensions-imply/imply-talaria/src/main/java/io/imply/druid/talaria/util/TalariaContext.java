/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.QueryContext;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Class for all MSQ context params
 */
public class TalariaContext
{
  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  public static final List<String> CTX_MAX_NUM_TASKS_LEGACY_ALIASES = ImmutableList.of("msqMaxNumTasks", "msqNumTasks");
  @VisibleForTesting
  static final int DEFAULT_MAX_NUM_TASKS = 2;

  public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
  public static final List<String> CTX_TASK_ASSIGNMENT_STRATEGY_LEGACY_ALIASES = ImmutableList.of("msqTaskAssignment");
  private static final String DEFAULT_TASK_ASSIGNMENT_STRATEGY = WorkerAssignmentStrategy.MAX.toString();

  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
  public static final List<String> CTX_FINALIZE_AGGREGATIONS_LEGACY_ALIASES = ImmutableList.of("msqFinalizeAggregations");

  public static final String CTX_ENABLE_DURABLE_SHUFFLE_STORAGE = "durableShuffleStorage";
  public static final List<String> CTX_ENABLE_DURABLE_SHUFFLE_STORAGE_LEGACY_ALIASES = ImmutableList.of("msqDurableShuffleStorage");
  private static final String DEFAULT_ENABLE_DURABLE_SHUFFLE_STORAGE = "false";

  public static final String CTX_DESTINATION = "destination";
  public static final List<String> CTX_DESTINATION_LEGACY_ALIASES = ImmutableList.of("msqDestination");
  private static final String DEFAULT_DESTINATION = null;

  public static final String CTX_ROWS_PER_SEGMENT = "rowsPerSegment";
  public static final List<String> CTX_ROWS_PER_SEGMENT_LEGACY_ALIASES = ImmutableList.of("msqRowsPerSegment");

  public static final String CTX_ROWS_IN_MEMORY = "rowsInMemory";
  public static final List<String> CTX_ROWS_IN_MEMORY_LEGACY_ALIASES = ImmutableList.of("msqRowsInMemory");

  public static boolean isDurableStorageEnabled(Map<String, Object> propertyMap)
  {
    return Boolean.parseBoolean(
        String.valueOf(
            getValueFromPropertyMap(
                propertyMap,
                TalariaContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE,
                TalariaContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE_LEGACY_ALIASES,
                DEFAULT_ENABLE_DURABLE_SHUFFLE_STORAGE
            )
        )
    );
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return Numbers.parseBoolean(
        getValueFromPropertyMap(
            queryContext.getMergedParams(),
            TalariaContext.CTX_FINALIZE_AGGREGATIONS,
            TalariaContext.CTX_FINALIZE_AGGREGATIONS_LEGACY_ALIASES,
            true
        )
    );
  }

  public static WorkerAssignmentStrategy getAssignmentStrategy(final QueryContext queryContext)
  {
    String assignmentStrategyString = (String) getValueFromPropertyMap(
        queryContext.getMergedParams(),
        TalariaContext.CTX_TASK_ASSIGNMENT_STRATEGY,
        TalariaContext.CTX_TASK_ASSIGNMENT_STRATEGY_LEGACY_ALIASES,
        DEFAULT_TASK_ASSIGNMENT_STRATEGY
    );

    return WorkerAssignmentStrategy.fromString(assignmentStrategyString);
  }

  public static int getMaxNumTasks(final QueryContext queryContext)
  {
    return Numbers.parseInt(
        getValueFromPropertyMap(
            queryContext.getMergedParams(),
            TalariaContext.CTX_MAX_NUM_TASKS,
            TalariaContext.CTX_MAX_NUM_TASKS_LEGACY_ALIASES,
            DEFAULT_MAX_NUM_TASKS
        )
    );
  }

  public static Object getDestination(final QueryContext queryContext)
  {
    return getValueFromPropertyMap(
        queryContext.getMergedParams(),
        TalariaContext.CTX_DESTINATION,
        TalariaContext.CTX_DESTINATION_LEGACY_ALIASES,
        DEFAULT_DESTINATION
    );
  }

  public static int getRowsPerSegment(final QueryContext queryContext, int defaultRowsPerSegment)
  {
    return Numbers.parseInt(
        getValueFromPropertyMap(
            queryContext.getMergedParams(),
            TalariaContext.CTX_ROWS_PER_SEGMENT,
            TalariaContext.CTX_ROWS_PER_SEGMENT_LEGACY_ALIASES,
            defaultRowsPerSegment
        )
    );
  }

  public static int getRowsInMemory(final QueryContext queryContext, int defaultRowsInMemory)
  {
    return Numbers.parseInt(
        getValueFromPropertyMap(
            queryContext.getMergedParams(),
            TalariaContext.CTX_ROWS_IN_MEMORY,
            TalariaContext.CTX_ROWS_IN_MEMORY_LEGACY_ALIASES,
            defaultRowsInMemory
        )
    );
  }

  @Nullable
  public static Object getValueFromPropertyMap(
      Map<String, Object> propertyMap,
      String key,
      @Nullable List<String> legacyKeys,
      @Nullable Object defaultValue
  )
  {
    if (propertyMap.get(key) != null) {
      return propertyMap.get(key);
    }

    if (legacyKeys != null) {
      for (String legacyKey : legacyKeys) {
        if (propertyMap.get(legacyKey) != null) {
          return propertyMap.get(legacyKey);
        }
      }
    }

    return defaultValue;
  }
}
