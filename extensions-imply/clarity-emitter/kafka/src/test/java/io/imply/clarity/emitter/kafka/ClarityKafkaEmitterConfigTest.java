/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import java.util.Properties;

public class ClarityKafkaEmitterConfigTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final JsonConfigurator configurator = new JsonConfigurator(
      objectMapper,
      Validation.buildDefaultValidatorFactory().getValidator()
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNoTopic()
  {
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage("clarity.topic - may not be null");
    configurator.configurate(new Properties(), "clarity", ClarityKafkaEmitterConfig.class);
  }

  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    props.setProperty("clarity.topic", "druid-metrics");

    final ClarityKafkaEmitterConfig config = configurator.configurate(
        props,
        "clarity",
        ClarityKafkaEmitterConfig.class
    );

    Assert.assertEquals("druid-metrics", config.getTopic());
    Assert.assertEquals(ImmutableMap.of(), config.getProducerProperties());
    Assert.assertNull(config.getClusterName());
    Assert.assertFalse(config.isAnonymous());
    Assert.assertFalse(config.isEmitSegmentDimension());
    Assert.assertEquals(100, config.getSamplingRate());
    Assert.assertEquals(
        ImmutableSet.of("query/wait/time", "query/segment/time", "query/segmentAndCache/time"),
        config.getSampledMetrics()
    );
    Assert.assertEquals(
        ImmutableSet.of("druid/historical", "druid/peon", "druid/realtime"),
        config.getSampledNodeTypes()
    );
    Assert.assertEquals(ImmutableSet.of(), config.getCustomQueryDimensions());
    Assert.assertNull(config.getContext());
  }

  @Test
  public void testSettingEverything()
  {
    final Properties props = new Properties();
    props.setProperty("clarity.topic", "my-cool-topic");
    props.setProperty("clarity.producer.foo.bar", "baz");
    props.setProperty("clarity.producer.beep", "boop");
    props.setProperty("clarity.clusterName", "best-cluster-ever");
    props.setProperty("clarity.anonymous", "true");
    props.setProperty("clarity.emitSegmentDimension", "true");
    props.setProperty("clarity.samplingRate", "92");
    props.setProperty("clarity.sampledMetrics", "[\"arf\"]");
    props.setProperty("clarity.sampledNodeTypes", "[\"woof\"]");
    props.setProperty("clarity.customQueryDimensions", "[\"dim1\", \"dim2\"]");
    props.setProperty("clarity.context", "{\"accountId\": \"123-456-7890\"}");

    final ClarityKafkaEmitterConfig config = configurator.configurate(
        props,
        "clarity",
        ClarityKafkaEmitterConfig.class
    );

    Assert.assertEquals("my-cool-topic", config.getTopic());
    Assert.assertEquals(ImmutableMap.of("foo.bar", "baz", "beep", "boop"), config.getProducerProperties());
    Assert.assertEquals("best-cluster-ever", config.getClusterName());
    Assert.assertTrue(config.isAnonymous());
    Assert.assertTrue(config.isEmitSegmentDimension());
    Assert.assertEquals(92, config.getSamplingRate());
    Assert.assertEquals(ImmutableSet.of("arf"), config.getSampledMetrics());
    Assert.assertEquals(ImmutableSet.of("woof"), config.getSampledNodeTypes());
    Assert.assertEquals(ImmutableSet.of("dim1", "dim2"), config.getCustomQueryDimensions());
    Assert.assertEquals(ImmutableMap.of("accountId", (Object) "123-456-7890"), config.getContext());
  }
}
