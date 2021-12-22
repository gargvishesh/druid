/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import java.util.Properties;

public class ClarityHttpEmitterConfigTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final JsonConfigurator configurator = new JsonConfigurator(
      objectMapper,
      Validation.buildDefaultValidatorFactory().getValidator()
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNoUrl()
  {
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage("clarity.recipientBaseUrl - may not be null");
    configurator.configurate(new Properties(), "clarity", ClarityHttpEmitterConfig.class);
  }

  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    props.setProperty("clarity.recipientBaseUrl", "http://example.com/");

    final ClarityHttpEmitterConfig config = configurator.configurate(
        props,
        "clarity",
        ClarityHttpEmitterConfig.class
    );

    Assert.assertEquals(60000, config.getFlushMillis());
    Assert.assertEquals(25, config.getFlushBufferPercentFull());
    Assert.assertEquals(500, config.getFlushCount());
    Assert.assertEquals(Long.MAX_VALUE, config.getFlushTimeOut());
    Assert.assertEquals("http://example.com/", config.getRecipientBaseUrl());
    Assert.assertNull(config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.ARRAY, config.getBatchingStrategy());
    Assert.assertEquals(5 * 1024 * 1024, config.getMaxBatchSize());
    Assert.assertEquals(100 * 1024 * 1024, config.getMaxBufferSize());
    Assert.assertEquals(new Period("PT1M"), config.getReadTimeout());
    Assert.assertNull(config.getClusterName());
    Assert.assertFalse(config.isAnonymous());
    Assert.assertFalse(config.isEmitSegmentDimension());
    Assert.assertEquals(ClarityHttpEmitterConfig.Compression.LZ4, config.getCompression());
    Assert.assertEquals(65536, config.getLz4BufferSize());
    Assert.assertEquals(100, config.getSamplingRate());
    Assert.assertEquals(
        ImmutableSet.of("query/wait/time", "query/segment/time", "query/segmentAndCache/time"),
        config.getSampledMetrics()
    );
    Assert.assertEquals(
        ImmutableSet.of("druid/historical", "druid/peon", "druid/realtime"),
        config.getSampledNodeTypes()
    );
    Assert.assertNull(config.getContext());
    Assert.assertNull(config.getWorkerCount());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("clarity.flushMillis", "30000");
    props.setProperty("clarity.flushBufferPercentFull", "20");
    props.setProperty("clarity.flushCount", "912");
    props.setProperty("clarity.flushTimeOut", "1234");
    props.setProperty("clarity.recipientBaseUrl", "http://my.url");
    props.setProperty("clarity.basicAuthentication", "myUserName:andPassword");
    props.setProperty("clarity.batchingStrategy", "NEWLINES");
    props.setProperty("clarity.maxBatchSize", "2024");
    props.setProperty("clarity.maxBufferSize", "2012");
    props.setProperty("clarity.timeOut", "PT7M");
    props.setProperty("clarity.clusterName", "best-cluster-ever");
    props.setProperty("clarity.anonymous", "true");
    props.setProperty("clarity.emitSegmentDimension", "true");
    props.setProperty("clarity.compression", "GZIP");
    props.setProperty("clarity.lz4BufferSize", "1113");
    props.setProperty("clarity.samplingRate", "92");
    props.setProperty("clarity.sampledMetrics", "[\"arf\"]");
    props.setProperty("clarity.sampledNodeTypes", "[\"woof\"]");
    props.setProperty("clarity.context", "{\"accountId\": \"123-456-7890\"}");
    props.setProperty("clarity.workerCount", "24");

    final ClarityHttpEmitterConfig config = configurator.configurate(
        props,
        "clarity",
        ClarityHttpEmitterConfig.class
    );

    Assert.assertEquals(30000, config.getFlushMillis());
    Assert.assertEquals(20, config.getFlushBufferPercentFull());
    Assert.assertEquals(912, config.getFlushCount());
    Assert.assertEquals(1234, config.getFlushTimeOut());
    Assert.assertEquals("http://my.url", config.getRecipientBaseUrl());
    Assert.assertEquals("myUserName:andPassword", config.getBasicAuthentication());
    Assert.assertEquals(BatchingStrategy.NEWLINES, config.getBatchingStrategy());
    Assert.assertEquals(2024, config.getMaxBatchSize());
    Assert.assertEquals(2012, config.getMaxBufferSize());
    Assert.assertEquals(new Period("PT7M"), config.getReadTimeout());
    Assert.assertEquals("best-cluster-ever", config.getClusterName());
    Assert.assertTrue(config.isAnonymous());
    Assert.assertTrue(config.isEmitSegmentDimension());
    Assert.assertEquals(ClarityHttpEmitterConfig.Compression.GZIP, config.getCompression());
    Assert.assertEquals(1113, config.getLz4BufferSize());
    Assert.assertEquals(92, config.getSamplingRate());
    Assert.assertEquals(ImmutableSet.of("arf"), config.getSampledMetrics());
    Assert.assertEquals(ImmutableSet.of("woof"), config.getSampledNodeTypes());
    Assert.assertEquals(ImmutableMap.of("accountId", (Object) "123-456-7890"), config.getContext());
    Assert.assertEquals((Integer) 24, config.getWorkerCount());
  }
}
