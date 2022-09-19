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
import org.apache.druid.query.QueryContext;
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
        ImmutableSet.of(),
        null
    );

    Map<String, Object> context = new HashMap<>();
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("druidFeature", "coolDruidFeature");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");

    QueryContext queryContext = new QueryContext(context);

    Map<String, String> expectedNonAnonymizedMap = new HashMap<>();
    expectedNonAnonymizedMap.put("dataSource", "eternity");
    expectedNonAnonymizedMap.put("implyView", "view");
    expectedNonAnonymizedMap.put("implyViewTitle", "truesight");
    expectedNonAnonymizedMap.put("implyFeature", "coolFeature");
    expectedNonAnonymizedMap.put("implyDataCube", "hypercube");
    expectedNonAnonymizedMap.put("druidFeature", "coolDruidFeature");
    expectedNonAnonymizedMap.put("implyUser", "implier");
    expectedNonAnonymizedMap.put("implyUserEmail", "implier@imply.io");

    Map<String, String> nonAnonymizedDimensions = new HashMap<>();
    nonAnonymizedDimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        nonAnonymizedConfig,
        nonAnonymizedDimensions::put,
        nonAnonymizedDimensions::get,
        queryContext
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
        ImmutableSet.of(),
        null
    );

    Map<String, Object> context = new HashMap<>();
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("druidFeature", "coolDruidFeature");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");

    QueryContext queryContext = new QueryContext(context);

    Map<String, String> expectedAnonymizedMap = new HashMap<>();
    expectedAnonymizedMap.put("dataSource", "eternity");
    expectedAnonymizedMap.put("implyView", "view");
    expectedAnonymizedMap.put("implyViewTitle", "truesight");
    expectedAnonymizedMap.put("implyFeature", "coolFeature");
    expectedAnonymizedMap.put("implyDataCube", "hypercube");
    expectedAnonymizedMap.put("druidFeature", "coolDruidFeature");
    expectedAnonymizedMap.put("implyUser", "998b903e421ca066f9aaead5e252f7421835d76e042945c58957240955db434f");
    expectedAnonymizedMap.put("implyUserEmail", "613e0a0334596486e8f40999f8624ad42cf4de67dfac614af635459f1bd3fae2");

    Map<String, String> anonymizedDimensions = new HashMap<>();
    anonymizedDimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        anonymizedConfig,
        anonymizedDimensions::put,
        anonymizedDimensions::get,
        queryContext
    );
    Assert.assertEquals(expectedAnonymizedMap, anonymizedDimensions);
  }

  @Test
  public void test_addContextDimensions_pivotParams()
  {
    TestClarityEmitterConfig anonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        true,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        null
    );

    Map<String, Object> pivotParams = new HashMap<>();
    pivotParams.put("implyView", "view");
    pivotParams.put("implyViewTitle", "truesight");
    pivotParams.put("implyFeature", "coolFeature");
    pivotParams.put("implyDataCube", "hypercube");
    pivotParams.put("implyUser", "implier");
    pivotParams.put("implyUserEmail", "implier@imply.io");

    Map<String, Object> userParams = new HashMap<>();
    userParams.put("pivotParams", pivotParams);

    Map<String, Object> context = new HashMap<>();
    context.put("userParams", userParams);
    context.put("druidFeature", "coolDruidFeature");

    QueryContext queryContext = new QueryContext(context);

    Map<String, String> expectedDimensionsMap = new HashMap<>();
    expectedDimensionsMap.put("dataSource", "eternity");
    expectedDimensionsMap.put("implyView", "view");
    expectedDimensionsMap.put("implyViewTitle", "truesight");
    expectedDimensionsMap.put("implyFeature", "coolFeature");
    expectedDimensionsMap.put("implyDataCube", "hypercube");
    expectedDimensionsMap.put("druidFeature", "coolDruidFeature");
    expectedDimensionsMap.put("implyUser", "998b903e421ca066f9aaead5e252f7421835d76e042945c58957240955db434f");
    expectedDimensionsMap.put("implyUserEmail", "613e0a0334596486e8f40999f8624ad42cf4de67dfac614af635459f1bd3fae2");

    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        anonymizedConfig,
        dimensions::put,
        dimensions::get,
        queryContext
    );
    Assert.assertEquals(expectedDimensionsMap, dimensions);
  }

  @Test
  public void test_addContextDimensions_no_pivotParams()
  {
    TestClarityEmitterConfig anonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        true,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        null
    );

    Map<String, Object> userParams = new HashMap<>();

    Map<String, Object> context = new HashMap<>();
    context.put("userParams", userParams);
    context.put("druidFeature", "coolDruidFeature");
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");

    QueryContext queryContext = new QueryContext(context);

    Map<String, String> expectedDimensionsMap = new HashMap<>();
    expectedDimensionsMap.put("dataSource", "eternity");
    expectedDimensionsMap.put("implyView", "view");
    expectedDimensionsMap.put("implyViewTitle", "truesight");
    expectedDimensionsMap.put("implyFeature", "coolFeature");
    expectedDimensionsMap.put("implyDataCube", "hypercube");
    expectedDimensionsMap.put("druidFeature", "coolDruidFeature");
    expectedDimensionsMap.put("implyUser", "998b903e421ca066f9aaead5e252f7421835d76e042945c58957240955db434f");
    expectedDimensionsMap.put("implyUserEmail", "613e0a0334596486e8f40999f8624ad42cf4de67dfac614af635459f1bd3fae2");

    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        anonymizedConfig,
        dimensions::put,
        dimensions::get,
        queryContext
    );
    Assert.assertEquals(expectedDimensionsMap, dimensions);
  }

  @Test
  public void test_addContextDimensions_implyX_in_pivotParams_and_context()
  {
    TestClarityEmitterConfig anonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        true,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        null
    );

    Map<String, Object> pivotParams = new HashMap<>();
    pivotParams.put("implyView", "view");
    pivotParams.put("implyViewTitle", "truesight");
    pivotParams.put("implyFeature", "coolFeature");
    pivotParams.put("implyDataCube", "hypercube");
    pivotParams.put("implyUser", "implier");
    pivotParams.put("implyUserEmail", "implier@imply.io");

    Map<String, Object> userParams = new HashMap<>();
    userParams.put("pivotParams", pivotParams);

    Map<String, Object> context = new HashMap<>();
    context.put("userParams", userParams);
    context.put("implyView", "view");
    context.put("implyViewTitle", "truesight");
    context.put("implyFeature", "coolFeature");
    context.put("implyDataCube", "hypercube");
    context.put("implyUser", "implier");
    context.put("implyUserEmail", "implier@imply.io");
    context.put("druidFeature", "coolDruidFeature");

    QueryContext queryContext = new QueryContext(context);

    Map<String, String> expectedDimensionsMap = new HashMap<>();
    expectedDimensionsMap.put("dataSource", "eternity");
    expectedDimensionsMap.put("implyView", "view");
    expectedDimensionsMap.put("implyViewTitle", "truesight");
    expectedDimensionsMap.put("implyFeature", "coolFeature");
    expectedDimensionsMap.put("implyDataCube", "hypercube");
    expectedDimensionsMap.put("druidFeature", "coolDruidFeature");
    expectedDimensionsMap.put("implyUser", "998b903e421ca066f9aaead5e252f7421835d76e042945c58957240955db434f");
    expectedDimensionsMap.put("implyUserEmail", "613e0a0334596486e8f40999f8624ad42cf4de67dfac614af635459f1bd3fae2");

    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("dataSource", "eternity");
    ClarityMetricsUtils.addContextDimensions(
        anonymizedConfig,
        dimensions::put,
        dimensions::get,
        queryContext
    );
    Assert.assertEquals(expectedDimensionsMap, dimensions);
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
        ImmutableSet.of(),
        null
    );

    Map<String, Object> userParams = ImmutableMap.of(
        QueryContexts.LANE_KEY, "some_lane",
        QueryContexts.PRIORITY_KEY, QueryContexts.DEFAULT_PRIORITY,
        "some_random_key", "wat"
    );

    QueryContext context = new QueryContext(userParams);

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
