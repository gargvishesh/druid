/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static io.imply.druid.talaria.util.TalariaContext.CTX_DESTINATION;
import static io.imply.druid.talaria.util.TalariaContext.CTX_DESTINATION_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_FINALIZE_AGGREGATIONS;
import static io.imply.druid.talaria.util.TalariaContext.CTX_FINALIZE_AGGREGATIONS_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_MAX_NUM_TASKS;
import static io.imply.druid.talaria.util.TalariaContext.CTX_MAX_NUM_TASKS_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ROWS_IN_MEMORY;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ROWS_IN_MEMORY_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ROWS_PER_SEGMENT;
import static io.imply.druid.talaria.util.TalariaContext.CTX_ROWS_PER_SEGMENT_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.CTX_TASK_ASSIGNMENT_STRATEGY;
import static io.imply.druid.talaria.util.TalariaContext.CTX_TASK_ASSIGNMENT_STRATEGY_LEGACY_ALIASES;
import static io.imply.druid.talaria.util.TalariaContext.DEFAULT_MAX_NUM_TASKS;

public class TalariaContextTest
{
  @Test
  public void isDurableStorageEnabled_noParameterSetReturnsDefaultValue()
  {
    Assert.assertFalse(TalariaContext.isDurableStorageEnabled(ImmutableMap.of()));
  }

  @Test
  public void isDurableStorageEnabled_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ENABLE_DURABLE_SHUFFLE_STORAGE, "true");
    Assert.assertTrue(TalariaContext.isDurableStorageEnabled(propertyMap));
  }

  @Test
  public void isDurableStorageEnabled_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ENABLE_DURABLE_SHUFFLE_STORAGE_LEGACY_ALIASES.get(0), "true");
    Assert.assertTrue(TalariaContext.isDurableStorageEnabled(propertyMap));
  }

  @Test
  public void isDurableStorageEnabled_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_ENABLE_DURABLE_SHUFFLE_STORAGE, "true",
        CTX_ENABLE_DURABLE_SHUFFLE_STORAGE_LEGACY_ALIASES.get(0), "false");
    Assert.assertTrue(TalariaContext.isDurableStorageEnabled(propertyMap));
  }

  @Test
  public void isFinalizeAggregations_noParameterSetReturnsDefaultValue()
  {
    Assert.assertTrue(TalariaContext.isFinalizeAggregations(new QueryContext()));
  }

  @Test
  public void isFinalizeAggregations_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_FINALIZE_AGGREGATIONS, "false");
    Assert.assertFalse(TalariaContext.isFinalizeAggregations(new QueryContext(propertyMap)));
  }

  @Test
  public void isFinalizeAggregations_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_FINALIZE_AGGREGATIONS_LEGACY_ALIASES.get(0), "false");
    Assert.assertFalse(TalariaContext.isFinalizeAggregations(new QueryContext(propertyMap)));
  }

  @Test
  public void isFinalizeAggregations_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_FINALIZE_AGGREGATIONS, "false",
        CTX_FINALIZE_AGGREGATIONS_LEGACY_ALIASES.get(0), "true");
    Assert.assertFalse(TalariaContext.isFinalizeAggregations(new QueryContext(propertyMap)));
  }

  @Test
  public void getAssignmentStrategy_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, TalariaContext.getAssignmentStrategy(new QueryContext()));
  }

  @Test
  public void getAssignmentStrategy_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TASK_ASSIGNMENT_STRATEGY, "AUTO");
    Assert.assertEquals(WorkerAssignmentStrategy.AUTO, TalariaContext.getAssignmentStrategy(new QueryContext(propertyMap)));
  }

  @Test
  public void getAssignmentStrategy_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TASK_ASSIGNMENT_STRATEGY_LEGACY_ALIASES.get(0), "AUTO");
    Assert.assertEquals(WorkerAssignmentStrategy.AUTO, TalariaContext.getAssignmentStrategy(new QueryContext(propertyMap)));
  }

  @Test
  public void getAssignmentStrategy_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_TASK_ASSIGNMENT_STRATEGY, "AUTO",
        CTX_TASK_ASSIGNMENT_STRATEGY_LEGACY_ALIASES.get(0), "MAX");
    Assert.assertEquals(WorkerAssignmentStrategy.AUTO, TalariaContext.getAssignmentStrategy(new QueryContext(propertyMap)));
  }

  @Test
  public void getMaxNumTasks_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(DEFAULT_MAX_NUM_TASKS, TalariaContext.getMaxNumTasks(new QueryContext()));
  }

  @Test
  public void getMaxNumTasks_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS, 101);
    Assert.assertEquals(101, TalariaContext.getMaxNumTasks(new QueryContext(propertyMap)));
  }

  @Test
  public void getMaxNumTasks_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS, 101);
    Assert.assertEquals(101, TalariaContext.getMaxNumTasks(new QueryContext(propertyMap)));
  }

  @Test
  public void getMaxNumTasks_olderParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS_LEGACY_ALIASES.get(0), 101);
    Assert.assertEquals(101, TalariaContext.getMaxNumTasks(new QueryContext(propertyMap)));
  }

  @Test
  public void getMaxNumTasks_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_MAX_NUM_TASKS, 11,
        CTX_MAX_NUM_TASKS_LEGACY_ALIASES.get(0), 101);
    Assert.assertEquals(11, TalariaContext.getMaxNumTasks(new QueryContext(propertyMap)));
  }

  @Test
  public void getDestination_noParameterSetReturnsDefaultValue()
  {
    Assert.assertNull(TalariaContext.getDestination(new QueryContext()));
  }

  @Test
  public void getDestination_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_DESTINATION, "dataSource");
    Assert.assertEquals("dataSource", TalariaContext.getDestination(new QueryContext(propertyMap)));
  }

  @Test
  public void getDestination_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_DESTINATION_LEGACY_ALIASES.get(0), "dataSource");
    Assert.assertEquals("dataSource", TalariaContext.getDestination(new QueryContext(propertyMap)));
  }

  @Test
  public void getDestination_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_DESTINATION, "dataSource",
        CTX_DESTINATION_LEGACY_ALIASES.get(0), "taskReport");
    Assert.assertEquals("dataSource", TalariaContext.getDestination(new QueryContext(propertyMap)));
  }

  @Test
  public void getRowsPerSegment_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(1000, TalariaContext.getRowsPerSegment(new QueryContext(), 1000));
  }

  @Test
  public void getRowsPerSegment_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_PER_SEGMENT, 10);
    Assert.assertEquals(10, TalariaContext.getRowsPerSegment(new QueryContext(propertyMap), 1000));
  }

  @Test
  public void getRowsPerSegment_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_PER_SEGMENT_LEGACY_ALIASES.get(0), 10);
    Assert.assertEquals(10, TalariaContext.getRowsPerSegment(new QueryContext(propertyMap), 1000));
  }

  @Test
  public void getRowsPerSegment_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_ROWS_PER_SEGMENT, 10,
        CTX_ROWS_PER_SEGMENT_LEGACY_ALIASES.get(0), 100);
    Assert.assertEquals(10, TalariaContext.getRowsPerSegment(new QueryContext(propertyMap), 1000));
  }

  @Test
  public void getRowsInMemory_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(1000, TalariaContext.getRowsInMemory(new QueryContext(), 1000));
  }

  @Test
  public void getRowsInMemory_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_IN_MEMORY, 10);
    Assert.assertEquals(10, TalariaContext.getRowsInMemory(new QueryContext(propertyMap), 1000));
  }

  @Test
  public void getRowsInMemory_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_IN_MEMORY_LEGACY_ALIASES.get(0), 10);
    Assert.assertEquals(10, TalariaContext.getRowsInMemory(new QueryContext(propertyMap), 1000));
  }

  @Test
  public void getRowsInMemory_bothParametersSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(
        CTX_ROWS_IN_MEMORY, 10,
        CTX_ROWS_IN_MEMORY_LEGACY_ALIASES.get(0), 1000);
    Assert.assertEquals(10, TalariaContext.getRowsInMemory(new QueryContext(propertyMap), 1000));
  }
}
