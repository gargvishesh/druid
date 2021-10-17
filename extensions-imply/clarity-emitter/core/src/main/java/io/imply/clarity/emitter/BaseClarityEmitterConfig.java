/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter;

import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface BaseClarityEmitterConfig
{
  int DEFAULT_SAMPLING_RATE = 100;

  Set<String> DEFAULT_SAMPLED_NODE_TYPES = ImmutableSet.of(
      "druid/historical",
      "druid/peon",
      "druid/realtime"
  );

  Set<String> DEFAULT_SAMPLED_METRICS = ImmutableSet.of(
      "query/wait/time",
      "query/segment/time",
      "query/segmentAndCache/time"
  );

  Set<String> DEFAULT_CUSTOM_QUERY_DIMENSIONS = Collections.emptySet();

  @Nullable
  String getClusterName();

  boolean isAnonymous();

  long getMaxBufferSize();

  boolean isEmitSegmentDimension();

  int getSamplingRate();

  Set<String> getSampledMetrics();

  Set<String> getSampledNodeTypes();

  Set<String> getCustomQueryDimensions();

  Map<String, Object> getContext();
}
