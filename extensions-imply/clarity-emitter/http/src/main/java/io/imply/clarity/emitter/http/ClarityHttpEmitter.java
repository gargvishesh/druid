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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.imply.clarity.emitter.ClarityEmitterUtils;
import io.imply.clarity.emitter.ClarityNodeDetails;
import io.netty.util.SuppressForbidden;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.utils.CompressionUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SuppressForbidden(reason = "Lists#newLinkedList")
public class ClarityHttpEmitter implements Flushable, Closeable, Emitter
{
  private static final Logger log = new Logger(ClarityHttpEmitter.class);
  private static final long BUFFER_FULL_WARNING_THROTTLE = 30000;
  private static final long CONSECUTIVE_TOO_MANY_REQUESTS_WARN = 12;
  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

  private final ClarityNodeDetails nodeDetails;
  private final ClarityHttpEmitterConfig config;
  private final HttpClient client;
  private final ObjectMapper jsonMapper;
  private final URL url;
  private final long bufferBytesForFlush;

  private final AtomicReference<List<byte[]>> eventsList = new AtomicReference<>(Lists.newLinkedList());
  private final AtomicInteger count = new AtomicInteger(0);
  private final AtomicLong bufferedSize = new AtomicLong(0);
  private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat(String.format(Locale.ENGLISH, "ClarityHttpEmitter-%s-%%s", INSTANCE_COUNTER.incrementAndGet()))
          .build()
  );
  private final AtomicLong version = new AtomicLong(0);
  private final AtomicLong consecutiveTooManyRequests = new AtomicLong(0);
  private final AtomicBoolean started = new AtomicBoolean(false);

  // This is used to prevent a deluge of runnables from being queued if bufferedSize exceeds bufferBytesForFlush and
  // triggers a flush. This is necessary since bufferedSize is not decremented until the end of the runnable once the
  // events have all been transmitted (since they're still occupying memory) and in the meantime, every new incoming
  // event would have caused a new runnable to queue which we want to avoid.
  private final AtomicInteger emittingRunnableRefCount = new AtomicInteger(0);

  // Trackers for buffer-full warnings. Only use under synchronized(eventsList).
  private long lastBufferFullWarning = 0;
  private long messagesDroppedSinceLastBufferFullWarning = 0;

  public ClarityHttpEmitter(
      final ClarityHttpEmitterConfig config,
      final ClarityNodeDetails nodeDetails,
      final HttpClient client,
      final ObjectMapper jsonMapper
  )
  {
    final int batchOverhead = config.getBatchingStrategy().batchStart().length +
                              config.getBatchingStrategy().batchEnd().length;

    Preconditions.checkArgument(
        config.getMaxBatchSize() >= ClarityEmitterUtils.getMaxEventSize() + batchOverhead,
        String.format(
            Locale.ENGLISH,
            "maxBatchSize must be greater than MAX_EVENT_SIZE[%,d] + overhead[%,d].",
            ClarityEmitterUtils.getMaxEventSize(),
            batchOverhead
        )
    );
    Preconditions.checkArgument(
        config.getMaxBufferSize() >= ClarityEmitterUtils.getMaxEventSize(),
        String.format(
            Locale.ENGLISH,
            "maxBufferSize must be greater than MAX_EVENT_SIZE[%,d].",
            ClarityEmitterUtils.getMaxEventSize()
        )
    );

    this.config = config;
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.bufferBytesForFlush = (long) (config.getMaxBufferSize() * (config.getFlushBufferPercentFull() / 100.0));
    this.nodeDetails = nodeDetails;

    try {
      this.url = new URL(config.getRecipientBaseUrl());
    }
    catch (MalformedURLException e) {
      throw new ISE(e, "Bad URL: %s", config.getRecipientBaseUrl());
    }
  }

  @LifecycleStart
  @Override
  public void start()
  {
    synchronized (started) {
      if (!started.getAndSet(true)) {
        log.info("%s started: %s, proxy[%s]", this.getClass().getSimpleName(), nodeDetails, config.getProxyConfig());
        exec.schedule(new ScheduledEmittingRunnable(version.get()), nextRunDelay(), TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    synchronized (started) {
      if (!started.get()) {
        throw new RejectedExecutionException("Service is closed.");
      }
    }

    final byte[] eventBytes = ClarityEmitterUtils.encodeEvent(event, nodeDetails, config, jsonMapper);
    if (eventBytes == null) {
      return;
    }

    synchronized (eventsList) {
      if (bufferedSize.get() + eventBytes.length <= config.getMaxBufferSize()) {
        eventsList.get().add(eventBytes);
        bufferedSize.addAndGet(eventBytes.length);

        if (count.incrementAndGet() >= config.getFlushCount() || bufferedSize.get() >= bufferBytesForFlush) {

          // Only queue a flush for flushCount or bufferBytesForFlush if one isn't already queued or running; otherwise
          // we will get spurious events queued since bufferedSize is not decremented until the end of the runnable.
          if (emittingRunnableRefCount.compareAndSet(0, 1)) {
            log.debug(
                "Triggering flush for flushCount [%,d >= %,d] or bufferBytesForFlush [%,d >= %,d]",
                count.get(),
                config.getFlushCount(),
                bufferedSize.get(),
                bufferBytesForFlush
            );
            exec.execute(new EmittingRunnable(version.get()));
          }
        }

      } else {
        messagesDroppedSinceLastBufferFullWarning++;
      }

      final long now = System.currentTimeMillis();
      if (messagesDroppedSinceLastBufferFullWarning > 0 && lastBufferFullWarning + BUFFER_FULL_WARNING_THROTTLE < now) {
        log.error("Buffer full: dropped %,d events!", messagesDroppedSinceLastBufferFullWarning);
        lastBufferFullWarning = now;
        messagesDroppedSinceLastBufferFullWarning = 0;
      }
    }
  }

  @Override
  public void flush() throws IOException
  {
    if (started.get()) {
      emittingRunnableRefCount.incrementAndGet();
      final Future future = exec.submit(new EmittingRunnable(version.get()));

      try {
        future.get(config.getFlushTimeOut(), TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e) {
        log.info("Thread interrupted");
        Thread.currentThread().interrupt();
        throw new IOException("Thread interrupted while flushing", e);
      }
      catch (ExecutionException e) {
        throw new IOException("Exception while flushing", e);
      }
      catch (TimeoutException e) {
        throw new IOException(
            String.format(Locale.ENGLISH, "Timed out after [%d] millis during flushing", config.getFlushTimeOut()),
            e
        );
      }
    }
  }

  @LifecycleStop
  @Override
  public void close() throws IOException
  {
    synchronized (started) {
      // flush() doesn't do things if it is not started, so flush must happen before we mark it as not started.
      flush();
      started.set(false);
      exec.shutdown();
    }
  }

  private class ScheduledEmittingRunnable extends EmittingRunnable
  {
    public ScheduledEmittingRunnable(long instantiatedVersion)
    {
      super(instantiatedVersion);
    }

    @Override
    public void run()
    {
      emittingRunnableRefCount.incrementAndGet();
      super.run();
    }
  }

  private class EmittingRunnable implements Runnable
  {
    private final long instantiatedVersion;

    public EmittingRunnable(
        long instantiatedVersion
    )
    {
      this.instantiatedVersion = instantiatedVersion;
    }

    @Override
    @SuppressForbidden(reason = "Lists#newLinkedList")
    public void run()
    {
      long currVersion = version.get();

      try {
        if (!started.get()) {
          log.info("Not started, skipping...");
          return;
        }

        if (instantiatedVersion != currVersion) {
          log.debug("Skipping because instantiatedVersion[%s] != currVersion[%s]", instantiatedVersion, currVersion);
          return;
        } else {
          count.set(0);
          currVersion = version.incrementAndGet();
        }

        final List<byte[]> events;
        synchronized (eventsList) {
          events = eventsList.getAndSet(Lists.newLinkedList());
        }

        long eventsBytesCount = 0;
        for (final byte[] message : events) {
          eventsBytesCount += message.length;
        }

        // At this point we have taken charge of "events" but have not yet decremented bufferedSize.
        // We must eventually either decrement bufferedSize or re-add the events to "eventsList".

        boolean requeue = false;

        try {
          final List<List<byte[]>> batches = splitIntoBatches(events);
          log.debug(
              "Running export with version[%s], eventsList count[%s], bytes[%s], batches[%s]",
              instantiatedVersion,
              events.size(),
              eventsBytesCount,
              batches.size()
          );

          for (final List<byte[]> batch : batches) {
            log.debug("Sending batch to url[%s], batch.size[%,d]", url, batch.size());

            final Request request = new Request(HttpMethod.POST, url)
                .setContent("application/json", maybeEncodeData(serializeBatch(batch)));

            if (ClarityHttpEmitterConfig.Compression.LZ4.equals(config.getCompression())) {
              request.setHeader("Content-Encoding", "x-lz4-java");
            } else if (ClarityHttpEmitterConfig.Compression.GZIP.equals(config.getCompression())) {
              request.setHeader("Content-Encoding", "gzip");
            }

            if (config.getBasicAuthentication() != null) {
              final String[] parts = config.getBasicAuthentication().split(":", 2);
              final String user = parts[0];
              final String password = parts.length > 0 ? parts[1] : "";
              request.setBasicAuthentication(user, password);
            }

            final StatusResponseHolder response = client.go(request, StatusResponseHandler.getInstance()).get();

            if (response.getStatus().getCode() == 401) {
              throw ClarityHttpException.warn(
                  "Received HTTP status 401 from [%s]. Please check your credentials in druid.emitter.clarity.basicAuthentication.",
                  config.getRecipientBaseUrl(),
                  config.getMaxBatchSize()
              );
            } else if (response.getStatus().getCode() == 413) {
              // Batch too large.
              throw ClarityHttpException.warn(
                  "Received HTTP status 413 from [%s]. Batch size of [%d] may be too large, try decreasing druid.emitter.clarity.maxBatchSize.",
                  config.getRecipientBaseUrl(),
                  config.getMaxBatchSize()
              );
            } else if (response.getStatus().getCode() == 429) {
              // Too many requests.
              if (consecutiveTooManyRequests.incrementAndGet() < CONSECUTIVE_TOO_MANY_REQUESTS_WARN) {
                throw ClarityHttpException.info(
                    "Received HTTP status 429 from [%s] with message [%s], trying again soon.",
                    config.getRecipientBaseUrl(),
                    response.getContent().trim()
                );
              } else {
                throw ClarityHttpException.warn(
                    "Received HTTP status 429 from [%s] with message [%s], trying again soon. Please contact Imply for more information.",
                    config.getRecipientBaseUrl(),
                    response.getContent().trim()
                );
              }
            } else if (response.getStatus().getCode() / 100 != 2) {
              // Catch-all for other errors.
              throw ClarityHttpException.warn(
                  "Received HTTP status %s from [%s] with message [%s]. Please contact Imply for more information.",
                  response.getStatus().getCode(),
                  config.getRecipientBaseUrl(),
                  response.getStatus().getReasonPhrase()
              );
            } else {
              // Success, clear consecutiveTooManyRequests marker.
              consecutiveTooManyRequests.set(0);
            }
          }
        }
        catch (ClarityHttpException e) {
          if (e.isQuiet()) {
            log.info(e.getMessage());
          } else {
            log.warn(e.getMessage());
          }
          requeue = true;
        }
        catch (Exception e) {
          log.warn(
              "Got exception when posting events to [%s], resubmitting. Exception was: %s",
              config.getRecipientBaseUrl(),
              e.toString()
          );
          requeue = true;
        }
        catch (Throwable e) {
          // Non-Exception Throwable. Don't retry, just throw away the messages and then re-throw.
          log.warn(
              "Got unrecoverable error when posting events to urlString [%s], dropping. Exception: %s",
              config.getRecipientBaseUrl(),
              e.getMessage()
          );
          throw e;
        }
        finally {
          if (requeue) {
            synchronized (eventsList) {
              eventsList.get().addAll(events);
            }
          } else {
            bufferedSize.addAndGet(-eventsBytesCount);
          }
        }
      }
      catch (Throwable e) {
        log.error(e, "Uncaught exception in EmittingRunnable.run()");
      }
      finally {
        emittingRunnableRefCount.decrementAndGet();
      }

      // Always reschedule, otherwise we all of a sudden don't emit anything.
      exec.schedule(new ScheduledEmittingRunnable(currVersion), nextRunDelay(), TimeUnit.MILLISECONDS);
    }

    /**
     * Serializes messages into a batch. Does not validate against maxBatchSize.
     *
     * @param messages list of JSON objects, one per message
     *
     * @return serialized JSON array
     */
    private byte[] serializeBatch(List<byte[]> messages)
    {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        boolean first = true;
        baos.write(config.getBatchingStrategy().batchStart());
        for (final byte[] message : messages) {
          if (first) {
            first = false;
          } else {
            baos.write(config.getBatchingStrategy().messageSeparator());
          }
          baos.write(message);
        }
        baos.write(config.getBatchingStrategy().batchEnd());
        return baos.toByteArray();
      }
      catch (IOException e) {
        // There's no reason to have IOException in the signature of this method, since BAOS won't throw them.
        throw Throwables.propagate(e);
      }
    }

    /**
     * Splits up messages into batches based on the configured maxBatchSize.
     *
     * @param messages list of JSON objects, one per message
     *
     * @return sub-lists of "messages"
     */
    @SuppressForbidden(reason = "Lists#newLinkedList")
    private List<List<byte[]>> splitIntoBatches(List<byte[]> messages)
    {
      final List<List<byte[]>> batches = Lists.newLinkedList();
      List<byte[]> currentBatch = new ArrayList<>();
      int currentBatchBytes = 0;

      for (final byte[] message : messages) {
        final int batchSizeAfterAddingMessage = config.getBatchingStrategy().batchStart().length
                                                + currentBatchBytes
                                                + config.getBatchingStrategy().messageSeparator().length
                                                + message.length
                                                + config.getBatchingStrategy().batchEnd().length;

        if (!currentBatch.isEmpty() && batchSizeAfterAddingMessage > config.getMaxBatchSize()) {
          // Existing batch is full; close it and start a new one.
          batches.add(currentBatch);
          currentBatch = new ArrayList<>();
          currentBatchBytes = 0;
        }

        currentBatch.add(message);
        currentBatchBytes += message.length;
      }

      if (!currentBatch.isEmpty()) {
        batches.add(currentBatch);
      }

      return batches;
    }
  }

  private byte[] maybeEncodeData(byte[] data) throws IOException
  {
    if (ClarityHttpEmitterConfig.Compression.NONE.equals(config.getCompression())) {
      return data;
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);

    if (ClarityHttpEmitterConfig.Compression.LZ4.equals(config.getCompression())) {
      try (LZ4BlockOutputStream lz4encoder = new LZ4BlockOutputStream(baos, config.getLz4BufferSize())) {
        lz4encoder.write(data);
      }
    } else if (ClarityHttpEmitterConfig.Compression.GZIP.equals(config.getCompression())) {
      CompressionUtils.gzip(new ByteArrayInputStream(data), baos);
    } else {
      throw new IAE("Unknown compression type %s", config.getCompression().name());
    }

    byte[] output = baos.toByteArray();
    log.debug("%s: %,d -> %,d bytes compressed", config.getCompression().name(), data.length, output.length);

    return output;
  }

  /**
   * Used for tests, should not be used elsewhere.
   *
   * @return the executor used for emission of events
   */
  @VisibleForTesting
  long getBufferedSize()
  {
    return bufferedSize.get();
  }

  /**
   * Used for tests, should not be used elsewhere.
   *
   * @return the executor used for emission of events
   */
  @VisibleForTesting
  ScheduledExecutorService getExec()
  {
    return exec;
  }

  private long nextRunDelay()
  {
    // Fuzz +/- 20% to avoid thundering herds.
    return (long) (config.getFlushMillis() * (ThreadLocalRandom.current().nextDouble() * 0.4 + 0.8));
  }
}
