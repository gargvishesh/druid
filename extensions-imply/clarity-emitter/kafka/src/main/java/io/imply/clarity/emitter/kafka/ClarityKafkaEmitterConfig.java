/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import io.imply.clarity.emitter.ClarityEmitterUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ClarityKafkaEmitterConfig implements BaseClarityEmitterConfig
{
  @JsonProperty("topic")
  @NotNull
  private String topic = null;

  @JsonProperty("producer")
  private Map<String, Object> producerProperties = new HashMap<>();

  @JsonProperty
  private String clusterName = null;

  @JsonProperty
  private boolean anonymous = false;

  @Min(0)
  @JsonProperty
  private long maxBufferSize = ClarityEmitterUtils.getMemoryBound();

  @JsonProperty
  private boolean emitSegmentDimension = false;

  @Min(0)
  @Max(100)
  @JsonProperty
  private int samplingRate = DEFAULT_SAMPLING_RATE;

  @JsonProperty
  private Set<String> sampledMetrics = DEFAULT_SAMPLED_METRICS;

  @JsonProperty
  private Set<String> sampledNodeTypes = DEFAULT_SAMPLED_NODE_TYPES;

  @JsonProperty
  private Set<String> customQueryDimensions = DEFAULT_CUSTOM_QUERY_DIMENSIONS;

  public ClarityKafkaEmitterConfig()
  {
    // For Jackson.
  }

  public String getTopic()
  {
    return topic;
  }

  public Map<String, String> getProducerProperties()
  {
    final Map<String, String> retVal = new HashMap<>();

    // Only include actual strings.
    for (Map.Entry<String, Object> entry : producerProperties.entrySet()) {
      if (entry.getValue() instanceof String) {
        retVal.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }

    return retVal;
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClarityKafkaEmitterConfig that = (ClarityKafkaEmitterConfig) o;
    return anonymous == that.anonymous &&
           maxBufferSize == that.maxBufferSize &&
           emitSegmentDimension == that.emitSegmentDimension &&
           samplingRate == that.samplingRate &&
           Objects.equals(topic, that.topic) &&
           Objects.equals(producerProperties, that.producerProperties) &&
           Objects.equals(clusterName, that.clusterName) &&
           Objects.equals(sampledMetrics, that.sampledMetrics) &&
           Objects.equals(sampledNodeTypes, that.sampledNodeTypes) &&
           Objects.equals(customQueryDimensions, that.customQueryDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        topic,
        producerProperties,
        clusterName,
        anonymous,
        maxBufferSize,
        emitSegmentDimension,
        samplingRate,
        sampledMetrics,
        sampledNodeTypes,
        customQueryDimensions
    );
  }

  @Override
  public String toString()
  {
    return "ClarityKafkaEmitterConfig{" +
           "topic='" + topic + '\'' +
           ", producerProperties=" + producerProperties +
           ", clusterName='" + clusterName + '\'' +
           ", anonymous=" + anonymous +
           ", maxBufferSize=" + maxBufferSize +
           ", emitSegmentDimension=" + emitSegmentDimension +
           ", samplingRate=" + samplingRate +
           ", sampledMetrics=" + sampledMetrics +
           ", sampledNodeTypes=" + sampledNodeTypes +
           ", customQueryDimensions=" + customQueryDimensions +
           '}';
  }
}
