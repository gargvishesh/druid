/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import com.google.common.collect.ImmutableMap;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import scala.Some;
import scala.collection.immutable.List$;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class TestBroker implements Closeable
{

  private final String zookeeperConnect;
  private final File directory;
  private final boolean directoryCleanup;
  private final int id;
  private final Map<String, String> brokerProps;

  private volatile KafkaServer server;

  public TestBroker(
      String zookeeperConnect,
      @Nullable File directory,
      int id,
      Map<String, String> brokerProps
  )
  {
    this.zookeeperConnect = zookeeperConnect;
    this.directory = directory == null ? FileUtils.createTempDir() : directory;
    this.directoryCleanup = directory == null;
    this.id = id;
    this.brokerProps = brokerProps == null ? ImmutableMap.of() : brokerProps;
  }

  public void start()
  {
    final Properties props = new Properties();
    props.setProperty("zookeeper.connect", zookeeperConnect);
    props.setProperty("zookeeper.session.timeout.ms", "30000");
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("log.dirs", directory.toString());
    props.setProperty("broker.id", String.valueOf(id));
    props.setProperty("port", String.valueOf(ThreadLocalRandom.current().nextInt(9999) + 10000));
    props.setProperty("advertised.host.name", "localhost");
    props.setProperty("offsets.topic.replication.factor", "1");
    props.putAll(brokerProps);

    final KafkaConfig config = new KafkaConfig(props);

    server = new KafkaServer(
        config,
        Time.SYSTEM,
        Some.apply(StringUtils.format("TestingBroker[%d]-", id)),
        List$.MODULE$.empty()
    );
    server.startup();
  }

  public int getPort()
  {
    return server.socketServer().config().port();
  }

  public KafkaConsumer<byte[], byte[]> newConsumer()
  {
    return new KafkaConsumer<>(consumerProperties());
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", StringUtils.format("localhost:%d", getPort()));
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());
    props.put("group.id", String.valueOf(ThreadLocalRandom.current().nextInt()));
    props.put("auto.offset.reset", "earliest");
    return props;
  }

  @Override
  public void close() throws IOException
  {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
    if (directoryCleanup) {
      org.apache.commons.io.FileUtils.forceDelete(directory);
    }
  }
}
