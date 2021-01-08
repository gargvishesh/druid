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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.imply.clarity.emitter.ClarityEmitterUtils;
import io.imply.clarity.emitter.ClarityNodeDetails;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ClarityKafkaEmitter implements Emitter
{
  private static final long KAFKA_GET_THROTTLE = 5000;
  private static final long ERROR_LOG_THROTTLE = 30000;
  private static final long FLUSH_TIMEOUT = 10000;
  private static final int DEFAULT_RETRIES = 3;

  private static Logger log = new Logger(ClarityKafkaEmitter.class);

  private final AtomicReference<Throwable> lostException = new AtomicReference<>();
  private final AtomicLong queuedCounter = new AtomicLong();
  private final AtomicLong sentCounter = new AtomicLong();
  private final AtomicLong ackedCounter = new AtomicLong();
  private final AtomicLong lostCounter = new AtomicLong();

  private final ClarityNodeDetails nodeDetails;
  private final ClarityKafkaEmitterConfig config;
  private final ClarityKafkaProducer<byte[], byte[]> producerHolder;
  private final ObjectMapper jsonMapper;
  private final MemoryBoundLinkedBlockingQueue<byte[]> queue;
  private final ExecutorService exec;

  private volatile boolean closed = false;

  // Synchronization to avoid too much error logging.
  private final Object errorLogLock = new Object();
  private long lastErrorLog = 0;

  public ClarityKafkaEmitter(
      final ClarityNodeDetails nodeDetails,
      final ClarityKafkaEmitterConfig config,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkArgument(
        config.getMaxBufferSize() >= ClarityEmitterUtils.getMaxEventSize() * 2,
        String.format(
            Locale.ENGLISH,
            "maxBufferSize must be greater than MAX_EVENT_SIZE[%,d] * 2.",
            ClarityEmitterUtils.getMaxEventSize()
        )
    );

    // Split maxBufferSize in two, give half to the Kafka producer and half to our queue.
    final long bufferMemoryShare = config.getMaxBufferSize() / 2;

    this.nodeDetails = nodeDetails;
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producerHolder = new ClarityKafkaProducer<>(
        () -> createKafkaProducer(config.getProducerProperties(), bufferMemoryShare)
    );
    this.queue = new MemoryBoundLinkedBlockingQueue<>(bufferMemoryShare);
    this.exec = Execs.singleThreaded("ClarityKafkaEmitter");
  }

  private static KafkaProducer<byte[], byte[]> createKafkaProducer(
      final Map<String, String> producerProperties,
      final long bufferMemory
  )
  {
    final ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(ClarityKafkaEmitter.class.getClassLoader());

      final Properties props = new Properties();
      props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(DEFAULT_RETRIES));
      props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferMemory));
      props.putAll(producerProperties);

      return new KafkaProducer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @LifecycleStart
  public void start()
  {
    exec.execute(this::sendToKafka);
    log.info("%s started: %s", this.getClass().getSimpleName(), nodeDetails);
  }

  private void sendToKafka()
  {
    try {
      KafkaProducer<byte[], byte[]> producer = null;
      while (!Thread.currentThread().isInterrupted() && producer == null) {
        try {
          producer = producerHolder.get();
        }
        catch (InterruptedException e) {
          // Re-throw so we exit quietly.
          throw e;
        }
        catch (Exception e) {
          log.warn(e, "Could not initialize Kafka producer, will try again soon.");
          Thread.sleep(KAFKA_GET_THROTTLE);
        }
      }

      while (!Thread.currentThread().isInterrupted()) {
        final MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> container = queue.take();
        final byte[] bytes = container.getData();

        producer.send(
            new ProducerRecord<>(config.getTopic(), bytes),
            (recordMetadata, e) -> {
              ackedCounter.incrementAndGet();

              if (e != null) {
                if (log.isDebugEnabled()) {
                  log.debug(e, "Message send failed: %s", new String(bytes, StandardCharsets.UTF_8));
                }

                lostException.compareAndSet(null, e);
                lostCounter.incrementAndGet();
              }
            }
        );

        sentCounter.incrementAndGet();
      }
    }
    catch (InterruptException | InterruptedException e) {
      // Interrupted, exit quietly.
    }
  }

  @Override
  public void emit(final Event event)
  {
    if (closed) {
      // Throw away messages that come after we're closed.
      return;
    }

    final byte[] eventBytes = ClarityEmitterUtils.encodeEvent(event, nodeDetails, config, jsonMapper);
    if (eventBytes == null) {
      return;
    }

    final MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> objectContainer =
        new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(
            eventBytes,
            eventBytes.length
        );

    if (queue.offer(objectContainer)) {
      sentCounter.incrementAndGet();
    } else {
      lostCounter.incrementAndGet();
    }

    synchronized (errorLogLock) {
      final long now = System.currentTimeMillis();
      final long lost = lostCounter.get();
      if (lost > 0 && lastErrorLog + ERROR_LOG_THROTTLE < now) {
        final Throwable e = lostException.getAndSet(null);
        if (e != null) {
          log.error(e, "Failed to emit to Kafka.");
        }

        log.error("Dropped %,d events.", lost);
        lastErrorLog = now;
        lostCounter.addAndGet(-lost);
      }
    }
  }

  @Override
  public void flush()
  {
    // Flush down to zero. This method is only called in tests and on "close", so this is reasonable behavior.
    // Timeout quietly after FLUSH_TIMEOUT.
    final long timeoutAt = System.currentTimeMillis() + FLUSH_TIMEOUT;
    while (ackedCounter.get() < sentCounter.get() && System.currentTimeMillis() < timeoutAt) {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    try {
      closed = true;
      flush();

      if (ackedCounter.get() < sentCounter.get()) {
        log.warn("Closed with %,d unacked messages.", ackedCounter.get());
      }

      producerHolder.close();
      exec.shutdownNow();
      exec.awaitTermination(10, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }
}
