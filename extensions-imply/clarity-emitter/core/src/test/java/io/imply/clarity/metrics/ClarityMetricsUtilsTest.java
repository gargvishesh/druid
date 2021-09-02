/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.clarity.TestClarityEmitterConfig;
import org.apache.druid.query.QueryContexts;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ClarityMetricsUtilsTest
{
  @Test
  public void test_addContextDimensions_non_anonymization()
  {
    TestClarityEmitterConfig nonAnonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        false,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of()
    );

    Map<String, Object> context = new HashMap<>();
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");

    Map<String, String> expectedNonAnonymizedMap = new HashMap<>();
    expectedNonAnonymizedMap.put("dataSource", "eternity");
    expectedNonAnonymizedMap.put("implyView", "view");
    expectedNonAnonymizedMap.put("implyViewTitle", "truesight");
    expectedNonAnonymizedMap.put("implyFeature", "coolFeature");
    expectedNonAnonymizedMap.put("implyDataCube", "hypercube");
    expectedNonAnonymizedMap.put("implyUser", "implier");
    expectedNonAnonymizedMap.put("implyUserEmail", "implier@imply.io");

    Map<String, String> nonAnonymizedDimensions = new HashMap<>();
    nonAnonymizedDimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        nonAnonymizedConfig,
        nonAnonymizedDimensions::put,
        nonAnonymizedDimensions::get,
        context
    );
    Assert.assertEquals(expectedNonAnonymizedMap, nonAnonymizedDimensions);
  }

  @Test
  public void test_addContextDimensions_anonymization()
  {
    TestClarityEmitterConfig anonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        true,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of()
    );

    Map<String, Object> context = new HashMap<>();
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");

    Map<String, String> expectedAnonymizedMap = new HashMap<>();
    expectedAnonymizedMap.put("dataSource", "eternity");
    expectedAnonymizedMap.put("implyView", "view");
    expectedAnonymizedMap.put("implyViewTitle", "truesight");
    expectedAnonymizedMap.put("implyFeature", "coolFeature");
    expectedAnonymizedMap.put("implyDataCube", "hypercube");
    expectedAnonymizedMap.put("implyUser", "998b903e421ca066f9aaead5e252f7421835d76e042945c58957240955db434f");
    expectedAnonymizedMap.put("implyUserEmail", "613e0a0334596486e8f40999f8624ad42cf4de67dfac614af635459f1bd3fae2");

    Map<String, String> anonymizedDimensions = new HashMap<>();
    anonymizedDimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        anonymizedConfig,
        anonymizedDimensions::put,
        anonymizedDimensions::get,
        context
    );
    Assert.assertEquals(expectedAnonymizedMap, anonymizedDimensions);
  }

  @Test
  public void test_priority_and_lane_in_context()
  {
    TestClarityEmitterConfig config = new TestClarityEmitterConfig(
        "cluster",
        false,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of()
    );

    Map<String, Object> context = ImmutableMap.of(
        QueryContexts.LANE_KEY, "some_lane",
        QueryContexts.PRIORITY_KEY, QueryContexts.DEFAULT_PRIORITY,
        "some_random_key", "wat"
    );

    Map<String, String> expected = ImmutableMap.of(
        QueryContexts.LANE_KEY, "some_lane",
        QueryContexts.PRIORITY_KEY, String.valueOf(QueryContexts.DEFAULT_PRIORITY)
    );
    Map<String, String> dimensions = new HashMap<>();

    ClarityMetricsUtils.addContextDimensions(
        config,
        dimensions::put,
        dimensions::get,
        context
    );
    Assert.assertEquals(expected, dimensions);
  }
}
