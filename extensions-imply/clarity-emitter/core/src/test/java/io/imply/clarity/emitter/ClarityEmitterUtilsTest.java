/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.clarity.TestClarityEmitterConfig;
import io.imply.clarity.TestEvent;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ClarityEmitterUtilsTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void test_getModifiedEventMap_non_anonymization()
  {
    TestEvent testEvent = new TestEvent(
        "walker",
        "theAbyss",
        "streamOfHistory",
        ImmutableMap.of()
    );

    ClarityNodeDetails nodeDetails = new ClarityNodeDetails(
        "broker",
        "compositeMind",
        "0.30.0",
        "2024.02"
    );

    TestClarityEmitterConfig nonAnonymizedConfig = new TestClarityEmitterConfig(
        "compositeMind",
        false,
        1000L,
        false,
        100,
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of("accountId", "123-456-7890")
    );

    Map<String, Object> nonAnonymizedMap = ClarityEmitterUtils.getModifiedEventMap(
        testEvent,
        nodeDetails,
        nonAnonymizedConfig,
        OBJECT_MAPPER
    );

    Assert.assertEquals(
        ImmutableMap.builder()
                    .put("feed", "streamOfHistory")
                    .put("metrics", ImmutableMap.of())
                    .put("implyNodeType", "broker")
                    .put("implyCluster", "compositeMind")
                    .put("implyDruidVersion", "0.30.0")
                    .put("implyVersion", "2024.02")
                    .put("identity", "walker")
                    .put("remoteAddress", "theAbyss")
                    .put("accountId", "123-456-7890").build(),
        nonAnonymizedMap
    );
  }

  @Test
  public void test_getModifiedEventMap_anonymization()
  {
    TestEvent testEvent = new TestEvent(
        "walker",
        "theAbyss",
        "streamOfHistory",
        ImmutableMap.of()
    );

    ClarityNodeDetails nodeDetails = new ClarityNodeDetails(
        "broker",
        "compositeMind",
        "0.30.0",
        "2024.02"
    );

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

    Map<String, Object> anonymizedMap = ClarityEmitterUtils.getModifiedEventMap(
        testEvent,
        nodeDetails,
        anonymizedConfig,
        OBJECT_MAPPER
    );

    Assert.assertEquals(
        ImmutableMap.builder()
                    .put("feed", "streamOfHistory")
                    .put("metrics", ImmutableMap.of())
                    .put("implyNodeType", "broker")
                    .put("implyCluster", "compositeMind")
                    .put("implyDruidVersion", "0.30.0")
                    .put("implyVersion", "2024.02")
                    .put("identity", "8be45f2a5ccabcb9fa7f4dda50ebd7f1ae46a9b45bc0a2d66a92a7c7ab36a47a")
                    .put("remoteAddress", "a7838e312ccac844b439f3922467a4b9e006227db82d0094690057a4fb3ac5ac").build(),
        anonymizedMap
    );
  }
}
