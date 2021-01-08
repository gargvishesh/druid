/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.concurrent.Callable;

public class ClarityKafkaProducer<K, V> implements Closeable
{
  private static final long KAFKA_CONSTRUCT_THROTTLE = 10000;
  private static final Logger log = new Logger(ClarityKafkaProducer.class);

  private final Callable<KafkaProducer<K, V>> producerMaker;

  // Only write under synchronized(lock).
  private volatile KafkaProducer<K, V> producer;

  // Only use at all under synchronized(this).
  private boolean closed = false;

  public ClarityKafkaProducer(final Callable<KafkaProducer<K, V>> producerMaker)
  {
    this.producerMaker = producerMaker;
  }

  @Nonnull
  public KafkaProducer<K, V> get() throws Exception
  {
    if (producer == null) {
      synchronized (this) {
        if (closed) {
          throw new IllegalStateException("We are closed and cannot make a producer.");
        }

        if (producer == null) {
          producer = producerMaker.call();
        }
      }
    }

    assert producer != null;
    return producer;
  }

  @Override
  public void close()
  {
    synchronized (this) {
      closed = true;
      if (producer != null) {
        producer.flush();
        producer.close();
      }
    }
  }
}
