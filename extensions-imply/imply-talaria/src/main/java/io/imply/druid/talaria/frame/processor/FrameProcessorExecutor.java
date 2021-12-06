/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.Try;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Manages execution of {@link FrameProcessor} in an {@link ExecutorService}.
 *
 * If you want single threaded execution, use {@code Execs.singleThreaded()}. It is not a good idea to use this with a
 * same-thread executor like {@code Execs.directExecutor()}, because it will lead to deep call stacks.
 */
public class FrameProcessorExecutor
{
  private static final Logger log = new Logger(FrameProcessorExecutor.class);

  private final ListeningExecutorService exec;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final SetMultimap<String, ListenableFuture<?>> cancellationFutures =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  @GuardedBy("lock")
  private final SetMultimap<String, FrameProcessor<?>> cancellationProcessors =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  @GuardedBy("lock")
  private final Set<FrameProcessor<?>> runningProcessors = Sets.newIdentityHashSet();

  public FrameProcessorExecutor(final ListeningExecutorService exec)
  {
    this.exec = exec;
  }

  /**
   * Returns the underlying executor service used by this executor.
   */
  public ListeningExecutorService getExecutorService()
  {
    return exec;
  }

  /**
   * Runs a processor until it is done, and returns a future that resolves when execution is complete.
   *
   * If "cancellationId" is provided, it can be used to cancel all processors currently running for a particular set of
   * work.
   */
  public <T> ListenableFuture<T> runFully(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    final List<ReadableFrameChannel> inputChannels = processor.inputChannels();
    final List<WritableFrameChannel> outputChannels = processor.outputChannels();
    final SettableFuture<T> finished = registerCancelableFuture(SettableFuture.create(), cancellationId);

    class ExecutorRunnable implements Runnable
    {
      private final AwaitAnyWidget awaitAnyWidget = new AwaitAnyWidget(inputChannels);

      @Override
      public void run()
      {
        try {
          final List<ListenableFuture<?>> allWritabilityFutures = gatherWritabilityFutures();
          final List<ListenableFuture<?>> writabilityFuturesToWaitFor =
              allWritabilityFutures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());

          logProcessorStatusString(processor, finished, allWritabilityFutures);

          if (!writabilityFuturesToWaitFor.isEmpty()) {
            runProcessorAfterFutureResolves(Futures.allAsList(writabilityFuturesToWaitFor));
            return;
          }

          final Optional<ReturnOrAwait<T>> maybeResult = runProcessorNow();

          if (!maybeResult.isPresent()) {
            // Processor was canceled. Just exit.
            return;
          }

          final ReturnOrAwait<T> result = maybeResult.get();
          logProcessorStatusString(processor, finished, null);

          if (result.isReturn()) {
            succeed(result.value());
          } else {
            final IntSet await = result.awaitSet();

            if (await.isEmpty()) {
              exec.submit(ExecutorRunnable.this);
            } else if (result.isAwaitAll() || await.size() == 1) {
              final List<ListenableFuture<?>> readabilityFutures = new ArrayList<>();

              for (final int channelNumber : await) {
                final ReadableFrameChannel channel = inputChannels.get(channelNumber);
                if (!channel.isFinished() && !channel.canRead()) {
                  readabilityFutures.add(channel.readabilityFuture());
                }
              }

              if (readabilityFutures.isEmpty()) {
                exec.submit(ExecutorRunnable.this);
              } else {
                runProcessorAfterFutureResolves(Futures.allAsList(readabilityFutures));
              }
            } else {
              // Await any.
              runProcessorAfterFutureResolves(awaitAnyWidget.awaitAny(await));
            }
          }
        }
        catch (Throwable e) {
          fail(e);
        }
      }

      private List<ListenableFuture<?>> gatherWritabilityFutures()
      {
        final List<ListenableFuture<?>> futures = new ArrayList<>();

        for (final WritableFrameChannel channel : outputChannels) {
          futures.add(channel.writabilityFuture());
        }

        return futures;
      }

      /**
       * Executes {@link FrameProcessor#runIncrementally} on the currently-readable inputs, while respecting
       * cancellation. Returns an empty Optional if the processor was canceled, or a present Optional if the processor
       * ran successfully. Throws an exception if the processor does.
       */
      private Optional<ReturnOrAwait<T>> runProcessorNow() throws IOException
      {
        final IntSet readableInputs = new IntOpenHashSet(inputChannels.size());

        for (int i = 0; i < inputChannels.size(); i++) {
          final ReadableFrameChannel channel = inputChannels.get(i);
          if (channel.isFinished() || channel.canRead()) {
            readableInputs.add(i);
          }
        }

        if (cancellationId != null) {
          synchronized (lock) {
            if (cancellationProcessors.containsEntry(cancellationId, processor)) {
              runningProcessors.add(processor);
            } else {
              // Processor has been canceled. We don't need to handle cleanup, because someone else did it.
              return Optional.empty();
            }
          }
        }

        // TODO(gianm): Interrupt the thread running this when cancel is called
        Optional<ReturnOrAwait<T>> retVal;

        try {
          retVal = Optional.of(processor.runIncrementally(readableInputs));
        }
        finally {
          if (cancellationId != null) {
            synchronized (lock) {
              runningProcessors.remove(processor);

              if (!cancellationProcessors.containsEntry(cancellationId, processor)) {
                // Processor has been canceled. We need to handle cleanup. Also: overwrite the return value.
                retVal = Optional.empty();
              }
            }
          }
        }

        if (!retVal.isPresent()) {
          fail(new ISE("Canceled"));
        }

        return retVal;
      }

      private <V> void runProcessorAfterFutureResolves(final ListenableFuture<V> future)
      {
        Futures.addCallback(
            registerCancelableFuture(future, cancellationId),
            new FutureCallback<V>()
            {
              @Override
              public void onSuccess(final V ignored)
              {
                try {
                  exec.submit(ExecutorRunnable.this);
                }
                catch (Throwable e) {
                  fail(e);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                fail(t);
              }
            }
        );
      }

      private void succeed(T value)
      {
        try {
          doProcessorCleanup();
        }
        catch (Throwable e) {
          finished.setException(e);
          return;
        }

        finished.set(value);
      }

      private void fail(Throwable e)
      {
        for (final WritableFrameChannel outputChannel : outputChannels) {
          try {
            outputChannel.write(Try.error(e));
          }
          catch (Throwable e1) {
            e.addSuppressed(e1);
          }
        }

        try {
          doProcessorCleanup();
        }
        catch (Throwable e1) {
          e.addSuppressed(e1);
        }

        finished.setException(e);
      }

      void doProcessorCleanup() throws IOException
      {
        if (cancellationId != null) {
          synchronized (lock) {
            cancellationProcessors.remove(cancellationId, processor);
          }
        }

        processor.cleanup();
      }
    }

    final ExecutorRunnable runnable = new ExecutorRunnable();

    finished.addListener(
        () -> {
          if (finished.isCancelled()) {
            try {
              cancel(cancellationId, processor);
            }
            catch (Throwable e) {
              // No point throwing. Exceptions thrown in listeners don't go anywhere.
              log.noStackTrace().warn(e, "Exception encountered while canceling processor [%s]", processor);
            }
          }

          logProcessorStatusString(processor, finished, null);
        },
        Execs.directExecutor()
    );

    logProcessorStatusString(processor, finished, null);
    registerCancelableProcessor(processor, cancellationId);
    exec.submit(runnable);
    return finished;
  }

  public void cancel(final String cancellationId)
  {
    Preconditions.checkNotNull(cancellationId, "cancellationId");

    final Set<ListenableFuture<?>> futuresToCancel;
    final Set<FrameProcessor<?>> processorsToCancel;

    synchronized (lock) {
      futuresToCancel = cancellationFutures.removeAll(cancellationId);
      processorsToCancel = Sets.newIdentityHashSet();
      processorsToCancel.addAll(cancellationProcessors.removeAll(cancellationId));

      // Don't cancel running processors; runProcessorNow will handle those.
      processorsToCancel.removeIf(runningProcessors::contains);
    }

    // Do the actual closing outside the critical section.
    final Closer closer = Closer.create();

    for (FrameProcessor<?> processor : processorsToCancel) {
      closer.register(processor::cleanup);
    }

    closer.register(() -> {
      for (final ListenableFuture<?> future : futuresToCancel) {
        // Best-effort cancellation, I suppose. The return value is ignored.
        future.cancel(true);
      }
    });

    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  <T, FutureType extends ListenableFuture<T>> FutureType registerCancelableFuture(
      final FutureType future,
      @Nullable final String cancellationId
  )
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancellationFutures.put(cancellationId, future);
        future.addListener(
            () -> {
              synchronized (lock) {
                cancellationFutures.remove(cancellationId, future);
              }
            },
            Execs.directExecutor()
        );
      }
    }

    return future;
  }

  private void cancel(final String cancellationId, final FrameProcessor<?> processor) throws IOException
  {
    synchronized (lock) {
      if (!cancellationProcessors.remove(cancellationId, processor) || runningProcessors.contains(processor)) {
        return;
      }
    }

    // Lack of return means "processor" is not running, and therefore won't run again, because we removed it from
    // cancellationProcessors. (runProcessorNow checks for that.) Run its cleanup routine.
    processor.cleanup();
  }

  private <T> void registerCancelableProcessor(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancellationProcessors.put(cancellationId, processor);
      }
    }
  }

  private static <T> void logProcessorStatusString(
      final FrameProcessor<T> processor,
      final ListenableFuture<?> finishedFuture,
      @Nullable final List<ListenableFuture<?>> writabilityFutures
  )
  {
    if (log.isDebugEnabled()) {
      final StringBuilder sb = new StringBuilder()
          .append("Processor [")
          .append(processor)
          .append("]; in=[");

      for (ReadableFrameChannel channel : processor.inputChannels()) {
        if (channel.canRead()) {
          sb.append("R"); // R for readable
        } else if (channel.isFinished()) {
          sb.append("D"); // D for done
        } else {
          sb.append("~"); // ~ for waiting
        }
      }

      sb.append("]");

      if (writabilityFutures != null) {
        sb.append("; out=[");

        for (final ListenableFuture<?> future : writabilityFutures) {
          if (future.isDone()) {
            sb.append("W"); // W for writable
          } else {
            sb.append("~"); // ~ for waiting
          }
        }

        sb.append("]");
      }

      sb.append("; cancel=").append(finishedFuture.isCancelled() ? "y" : "n");
      sb.append("; done=").append(finishedFuture.isDone() ? "y" : "n");

      log.debug(StringUtils.encodeForFormat(sb.toString()));
    }
  }
}
