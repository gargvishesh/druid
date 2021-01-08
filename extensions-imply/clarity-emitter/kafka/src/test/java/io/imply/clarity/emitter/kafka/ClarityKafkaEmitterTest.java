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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.clarity.emitter.ClarityNodeDetails;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.validation.Validation;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class ClarityKafkaEmitterTest
{
  private static TestingCluster ZK_SERVER;
  private static TestBroker KAFKA_SERVER;

  private final ClarityNodeDetails nodeDetails = new ClarityNodeDetails(
      "test/type",
      "testCluster",
      "1",
      "2"
  );
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final JsonConfigurator configurator = new JsonConfigurator(
      objectMapper,
      Validation.buildDefaultValidatorFactory().getValidator()
  );

  private ClarityKafkaEmitter emitter;

  @BeforeClass
  public static void setupClass() throws Exception
  {
    ZK_SERVER = new TestingCluster(1);
    ZK_SERVER.start();

    KAFKA_SERVER = new TestBroker(ZK_SERVER.getConnectString(), null, 1, ImmutableMap.of());
    KAFKA_SERVER.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    KAFKA_SERVER.close();
    KAFKA_SERVER = null;

    ZK_SERVER.stop();
    ZK_SERVER = null;
  }

  @After
  public void tearDown()
  {
    if (emitter != null) {
      emitter.close();
    }
  }

  @Test(timeout = 60_000L)
  public void testEmitter() throws Exception
  {
    final String topic = "test-" + UUID.randomUUID();
    final List<Event> events = ImmutableList.of(
        ServiceMetricEvent.builder()
                          .build(new DateTime("2000", DateTimeZone.UTC), "myMetric", 3.2)
                          .build("myService", "myHost"),
        AlertBuilder.create("My Alert").build("myService", "myHost")
    );

    emitToKafka(topic, events);
    final List<Map<String, Object>> eventsRead = readFromKafka(topic, events.size());

    Assert.assertEquals(
        events.stream().map(this::makeExpectedMap).collect(Collectors.toList()),
        eventsRead
    );
  }

  @Test(timeout = 60_000L)
  public void testNothingBadHappensWhenKafkaIsInvalid()
  {
    final String topic = "test-" + UUID.randomUUID();
    final List<Event> events = ImmutableList.of(
        ServiceMetricEvent.builder()
                          .build(new DateTime("2000", DateTimeZone.UTC), "myMetric", 3.2)
                          .build("myService", "myHost"),
        AlertBuilder.create("My Alert").build("myService", "myHost")
    );

    final ClarityKafkaEmitter emitter = makeEmitter(
        ImmutableMap.of(
            "clarity.producer.bootstrap.servers", "nonexistent.example.com:9999",
            "clarity.producer.acks", "all",
            "clarity.topic", topic
        )
    );

    try {
      events.forEach(emitter::emit);
    }
    finally {
      emitter.close();
    }

    Assert.assertTrue(true);
  }

  public void emitToKafka(final String topic, final List<Event> events)
  {
    final ClarityKafkaEmitter emitter = makeEmitter(
        ImmutableMap.of(
            "clarity.producer.bootstrap.servers", "localhost:" + KAFKA_SERVER.getPort(),
            "clarity.producer.acks", "all",
            "clarity.topic", topic
        )
    );

    try {
      events.forEach(emitter::emit);
    }
    finally {
      emitter.close();
    }
  }

  public List<Map<String, Object>> readFromKafka(final String topic, final int limit) throws IOException
  {
    try (final KafkaConsumer<byte[], byte[]> consumer = KAFKA_SERVER.newConsumer()) {
      final List<Map<String, Object>> retVal = new ArrayList<>();
      consumer.subscribe(Collections.singletonList(topic));

      while (retVal.size() < limit) {
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(60));
        if (records.isEmpty()) {
          throw new ISE("Did not read all records, expected %,d but got %,d.", limit, retVal.size());
        }

        for (ConsumerRecord<byte[], byte[]> record : records) {
          final Map<String, Object> map = objectMapper.readValue(
              record.value(),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          );
          retVal.add(map);
        }
      }

      return retVal;
    }
  }

  public Map<String, Object> makeExpectedMap(final Event event)
  {
    final Map<String, Object> m = new HashMap<>(event.toMap());
    m.put("implyNodeType", "test/type");
    m.put("implyCluster", "testCluster");
    m.put("implyDruidVersion", "1");
    m.put("implyVersion", "2");
    return m;
  }

  public ClarityKafkaEmitter makeEmitter(final Map<String, String> configMap)
  {
    final ClarityKafkaEmitter emitter = new ClarityKafkaEmitter(
        nodeDetails,
        makeConfig(configMap),
        objectMapper
    );
    emitter.start();
    return emitter;
  }

  public ClarityKafkaEmitterConfig makeConfig(final Map<String, String> configMap)
  {
    final Properties props = new Properties();
    configMap.forEach(props::setProperty);
    return configurator.configurate(props, "clarity", ClarityKafkaEmitterConfig.class);
  }
}
