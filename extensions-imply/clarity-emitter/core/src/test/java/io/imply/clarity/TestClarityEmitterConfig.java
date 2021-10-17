/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity;

import io.imply.clarity.emitter.BaseClarityEmitterConfig;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public class TestClarityEmitterConfig implements BaseClarityEmitterConfig
{
  private final String clusterName;
  private final Boolean anonymous;
  private final Long maxBufferSize;
  private final Boolean emitSegmentDimension;
  private final Integer samplingRate;
  private final Set<String> sampledMetrics;
  private final Set<String> sampledNodeTypes;
  private final Set<String> customQueryDimensions;
  private final Map<String, Object> context;

  public TestClarityEmitterConfig(
      String clusterName,
      Boolean anonymous,
      Long maxBufferSize,
      Boolean emitSegmentDimension,
      Integer samplingRate,
      Set<String> sampledMetrics,
      Set<String> sampledNodeTypes,
      Set<String> customQueryDimensions,
      Map<String, Object> context
  )
  {
    this.clusterName = clusterName;
    this.anonymous = anonymous;
    this.maxBufferSize = maxBufferSize;
    this.emitSegmentDimension = emitSegmentDimension;
    this.samplingRate = samplingRate;
    this.sampledMetrics = sampledMetrics;
    this.sampledNodeTypes = sampledNodeTypes;
    this.customQueryDimensions = customQueryDimensions;
    this.context = context;
  }

  @Nullable
  @Override
  public String getClusterName()
  {
    return clusterName;
  }

  @Override
  public boolean isAnonymous()
  {
    return anonymous;
  }

  @Override
  public long getMaxBufferSize()
  {
    return maxBufferSize;
  }

  @Override
  public boolean isEmitSegmentDimension()
  {
    return emitSegmentDimension;
  }

  @Override
  public int getSamplingRate()
  {
    return samplingRate;
  }

  @Override
  public Set<String> getSampledMetrics()
  {
    return sampledMetrics;
  }

  @Override
  public Set<String> getSampledNodeTypes()
  {
    return sampledNodeTypes;
  }

  @Override
  public Set<String> getCustomQueryDimensions()
  {
    return customQueryDimensions;
  }

  @Override
  public Map<String, Object> getContext()
  {
    return context;
  }
}
