/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import io.imply.clarity.emitter.ClarityEmitterUtils;
import org.apache.druid.java.util.http.client.HttpClientProxyConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ClarityHttpEmitterConfig implements BaseClarityEmitterConfig
{
  public enum Compression
  {
    NONE, LZ4, GZIP
  }

  private static final int DEFAULT_FLUSH_MILLIS = 60 * 1000;
  private static final int DEFAULT_FLUSH_COUNT = 500;
  private static final int DEFAULT_MAX_BATCH_SIZE = 5 * 1024 * 1024;
  private static final Period DEFAULT_TIME_OUT = new Period("PT1M");
  private static final long DEFAULT_FLUSH_TIME_OUT = Long.MAX_VALUE; // do not time out in case flushTimeOut is not set
  private static final int DEFAULT_FLUSH_BUFFER_PERCENT_FULL = 25;
  private static final BatchingStrategy DEFAULT_BATCHING_STRATEGY = BatchingStrategy.ARRAY;
  private static final Compression DEFAULT_COMPRESSION = Compression.LZ4;
  private static final int DEFAULT_LZ4_BUFFER_SIZE = 65536;

  @Min(1)
  @JsonProperty
  private long flushMillis = DEFAULT_FLUSH_MILLIS;

  @Min(1)
  @Max(100)
  @JsonProperty
  private int flushBufferPercentFull = DEFAULT_FLUSH_BUFFER_PERCENT_FULL;

  @Min(0)
  @JsonProperty
  private int flushCount = DEFAULT_FLUSH_COUNT;

  @Min(0)
  @JsonProperty
  private long flushTimeOut = DEFAULT_FLUSH_TIME_OUT;

  @NotNull
  @JsonProperty(required = true)
  private String recipientBaseUrl = null;

  @JsonProperty
  private String basicAuthentication = null;

  @JsonProperty
  private BatchingStrategy batchingStrategy = DEFAULT_BATCHING_STRATEGY;

  @Min(0)
  @JsonProperty
  private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

  @Min(0)
  @JsonProperty
  private long maxBufferSize = ClarityEmitterUtils.getMemoryBound();

  @JsonProperty
  private Period timeOut = DEFAULT_TIME_OUT;

  @JsonProperty
  private String clusterName = null;

  @JsonProperty
  private boolean anonymous = false;

  @JsonProperty
  private Compression compression = DEFAULT_COMPRESSION;

  @Min(64)
  @Max(500000)
  @JsonProperty
  private int lz4BufferSize = DEFAULT_LZ4_BUFFER_SIZE;

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
  private Set<String> customQueryDimensions = Collections.emptySet();

  @JsonProperty("proxy")
  private HttpClientProxyConfig proxyConfig = null;

  @JsonProperty
  private Map<String, Object> context = null;
  
  @JsonProperty
  private Integer workerCount;

  private ClarityHttpEmitterConfig()
  {
    // For Jackson.
  }

  private ClarityHttpEmitterConfig(
      Long flushMillis,
      Integer flushBufferPercentFull,
      Integer flushCount,
      Long flushTimeOut,
      String recipientBaseUrl,
      String basicAuthentication,
      BatchingStrategy batchingStrategy,
      Integer maxBatchSize,
      Long maxBufferSize,
      Period timeOut,
      String clusterName,
      Boolean anonymous,
      Compression compression,
      Integer lz4BufferSize,
      Boolean emitSegmentDimension,
      Integer samplingRate,
      Set<String> sampledMetrics,
      Set<String> sampledNodeTypes,
      Set<String> customQueryDimensions,
      HttpClientProxyConfig proxyConfig,
      Map<String, Object> context,
      Integer workerCount
  )
  {
    this.flushMillis = (flushMillis != null ? flushMillis : DEFAULT_FLUSH_MILLIS);
    this.flushBufferPercentFull = (flushBufferPercentFull != null
                                   ? flushBufferPercentFull
                                   : DEFAULT_FLUSH_BUFFER_PERCENT_FULL);

    this.flushCount = (flushCount != null ? flushCount : DEFAULT_FLUSH_COUNT);
    this.flushTimeOut = (flushTimeOut != null ? flushTimeOut : DEFAULT_FLUSH_TIME_OUT);
    this.recipientBaseUrl = Preconditions.checkNotNull(recipientBaseUrl, "recipientBaseUrl");
    this.basicAuthentication = basicAuthentication;
    this.batchingStrategy = (batchingStrategy != null ? batchingStrategy : DEFAULT_BATCHING_STRATEGY);
    this.maxBatchSize = (maxBatchSize != null ? maxBatchSize : DEFAULT_MAX_BATCH_SIZE);
    this.maxBufferSize = (maxBufferSize != null ? maxBufferSize : ClarityEmitterUtils.getMemoryBound());
    this.timeOut = (timeOut != null ? timeOut : DEFAULT_TIME_OUT);
    this.clusterName = clusterName;
    this.anonymous = (anonymous != null ? anonymous : false);
    this.compression = (compression != null ? compression : DEFAULT_COMPRESSION);
    this.lz4BufferSize = (lz4BufferSize != null ? lz4BufferSize : DEFAULT_LZ4_BUFFER_SIZE);
    this.emitSegmentDimension = (emitSegmentDimension != null ? emitSegmentDimension : false);
    this.samplingRate = (samplingRate != null ? samplingRate : DEFAULT_SAMPLING_RATE);
    this.sampledMetrics = (sampledMetrics != null ? sampledMetrics : DEFAULT_SAMPLED_METRICS);
    this.sampledNodeTypes = (sampledNodeTypes != null ? sampledNodeTypes : DEFAULT_SAMPLED_NODE_TYPES);
    this.customQueryDimensions = (customQueryDimensions != null
                                  ? customQueryDimensions
                                  : DEFAULT_CUSTOM_QUERY_DIMENSIONS);
    this.proxyConfig = proxyConfig;
    this.context = context;
    this.workerCount = workerCount;
  }

  public long getFlushMillis()
  {
    return flushMillis;
  }

  public int getFlushBufferPercentFull()
  {
    return flushBufferPercentFull;
  }

  public int getFlushCount()
  {
    return flushCount;
  }

  public long getFlushTimeOut()
  {
    return flushTimeOut;
  }

  public String getRecipientBaseUrl()
  {
    return recipientBaseUrl;
  }

  public String getBasicAuthentication()
  {
    return basicAuthentication;
  }

  public BatchingStrategy getBatchingStrategy()
  {
    return batchingStrategy;
  }

  public int getMaxBatchSize()
  {
    return maxBatchSize;
  }

  @Override
  public long getMaxBufferSize()
  {
    return maxBufferSize;
  }

  public Period getReadTimeout()
  {
    return timeOut;
  }

  @Override
  @Nullable
  public String getClusterName()
  {
    return clusterName;
  }

  @Override
  public boolean isAnonymous()
  {
    return anonymous;
  }

  public Compression getCompression()
  {
    return compression;
  }

  public int getLz4BufferSize()
  {
    return lz4BufferSize;
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

  public HttpClientProxyConfig getProxyConfig()
  {
    return proxyConfig;
  }

  @Override
  public Map<String, Object> getContext()
  {
    return context;
  }

  public Integer getWorkerCount()
  {
    return workerCount;
  }

  @Override
  public String toString()
  {
    return "ClarityHttpEmitterConfig{" +
           "flushMillis=" + flushMillis +
           ", flushBufferPercentFull=" + flushBufferPercentFull +
           ", flushCount=" + flushCount +
           ", flushTimeOut=" + flushTimeOut +
           ", recipientBaseUrl='" + recipientBaseUrl + '\'' +
           ", basicAuthentication='" + (basicAuthentication == null ? "null" : "<redacted>") + '\'' +
           ", batchingStrategy=" + batchingStrategy +
           ", maxBatchSize=" + maxBatchSize +
           ", maxBufferSize=" + maxBufferSize +
           ", timeOut=" + timeOut +
           ", clusterName='" + clusterName + '\'' +
           ", anonymous=" + anonymous +
           ", compression=" + compression +
           ", lz4BufferSize=" + lz4BufferSize +
           ", emitSegmentDimension=" + emitSegmentDimension +
           ", samplingRate=" + samplingRate +
           ", sampledMetrics=" + sampledMetrics +
           ", sampledNodeTypes=" + sampledNodeTypes +
           ", customQueryDimensions=" + customQueryDimensions +
           ", proxyConfig=" + proxyConfig +
           ", context=" + context +
           ", workerCount=" + workerCount +
           '}';
  }

  public static Builder builder(String recipientBaseUrl)
  {
    return new Builder(recipientBaseUrl);
  }

  public static class Builder
  {
    private Long flushMillis;
    private Integer flushBufferPercentFull;
    private Integer flushCount;
    private Long flushTimeOut;
    private String recipientBaseUrl;
    private String basicAuthentication;
    private BatchingStrategy batchingStrategy;
    private Integer maxBatchSize;
    private Long maxBufferSize;
    private Period timeOut;
    private String clusterName;
    private Boolean anonymous;
    private Compression compression;
    private Integer lz4BufferSize;
    private Boolean emitSegmentDimension;
    private Integer samplingRate;
    private Set<String> sampledMetrics;
    private Set<String> sampledNodeTypes;
    private Set<String> customQueryDimensions;
    private HttpClientProxyConfig proxyConfig;
    private Map<String, Object> context;
    private Integer workerCount;

    public Builder(String recipientBaseUrl)
    {
      this.recipientBaseUrl = recipientBaseUrl;
    }

    public Builder withFlushMillis(long flushMillis)
    {
      this.flushMillis = flushMillis;
      return this;
    }

    public Builder withFlushBufferPercentFull(int flushBufferPercentFull)
    {
      this.flushBufferPercentFull = flushBufferPercentFull;
      return this;
    }

    public Builder withFlushCount(int flushCount)
    {
      this.flushCount = flushCount;
      return this;
    }

    public Builder withFlushTimeOut(long flushTimeOut)
    {
      this.flushTimeOut = flushTimeOut;
      return this;
    }

    public Builder withBasicAuthentication(String basicAuthentication)
    {
      this.basicAuthentication = basicAuthentication;
      return this;
    }

    public Builder withBatchingStrategy(BatchingStrategy batchingStrategy)
    {
      this.batchingStrategy = batchingStrategy;
      return this;
    }

    public Builder withMaxBatchSize(int maxBatchSize)
    {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder withMaxBufferSize(long maxBufferSize)
    {
      this.maxBufferSize = maxBufferSize;
      return this;
    }

    public Builder withTimeOut(Period timeOut)
    {
      this.timeOut = timeOut;
      return this;
    }

    public Builder withClusterName(String clusterName)
    {
      this.clusterName = clusterName;
      return this;
    }

    public Builder withAnonymous(boolean anonymous)
    {
      this.anonymous = anonymous;
      return this;
    }

    public Builder withCompression(Compression compression)
    {
      this.compression = compression;
      return this;
    }

    public Builder withLz4BufferSize(int lz4BufferSize)
    {
      this.lz4BufferSize = lz4BufferSize;
      return this;
    }

    public Builder withEmitSegmentDimension(boolean emitSegmentDimension)
    {
      this.emitSegmentDimension = emitSegmentDimension;
      return this;
    }

    public Builder withSamplingRate(int samplingRate)
    {
      this.samplingRate = samplingRate;
      return this;
    }

    public Builder withSampledMetrics(Set<String> sampledMetrics)
    {
      this.sampledMetrics = sampledMetrics;
      return this;
    }

    public Builder withSampledNodeTypes(Set<String> sampledNodeTypes)
    {
      this.sampledNodeTypes = sampledNodeTypes;
      return this;
    }

    public Builder withCustomQueryDimensions(Set<String> customQueryDimensions)
    {
      this.customQueryDimensions = customQueryDimensions;
      return this;
    }

    public Builder withProxyConfig(HttpClientProxyConfig proxyConfig)
    {
      this.proxyConfig = proxyConfig;
      return this;
    }

    public Builder withContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder withWorkerCount(Integer workerCount)
    {
      this.workerCount = workerCount;
      return this;
    }

    public ClarityHttpEmitterConfig build()
    {
      return new ClarityHttpEmitterConfig(
          flushMillis,
          flushBufferPercentFull,
          flushCount,
          flushTimeOut,
          recipientBaseUrl,
          basicAuthentication,
          batchingStrategy,
          maxBatchSize,
          maxBufferSize,
          timeOut,
          clusterName,
          anonymous,
          compression,
          lz4BufferSize,
          emitSegmentDimension,
          samplingRate,
          sampledMetrics,
          sampledNodeTypes,
          customQueryDimensions,
          proxyConfig,
          context,
          workerCount
      );
    }
  }
}
