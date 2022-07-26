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
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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

  // Futures that are active and therefore cancelable.
  @GuardedBy("lock")
  private final SetMultimap<String, ListenableFuture<?>> cancelableFutures =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  // Processors that are active and therefore cancelable. They may not currently be running on an actual thread.
  @GuardedBy("lock")
  private final SetMultimap<String, FrameProcessor<?>> cancelableProcessors =
      Multimaps.newSetMultimap(new HashMap<>(), Sets::newIdentityHashSet);

  // Processors that are currently running on a thread.
  // The "lock" is notified when processors are removed from runningProcessors.
  @GuardedBy("lock")
  private final Map<FrameProcessor<?>, Thread> runningProcessors = new IdentityHashMap<>();

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
   * If "cancellationId" is provided, it can be used with the {@link #cancel(String)} method to cancel all processors
   * currently running with the same cancellationId.
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
            // Processor exited abnormally. Just exit; cleanup would have been handled elsewhere.
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
       * cancellation. Returns an empty Optional if the processor exited abnormally (canceled or failed). Returns a
       * present Optional if the processor ran successfully. Throws an exception if the processor does.
       */
      private Optional<ReturnOrAwait<T>> runProcessorNow()
      {
        final IntSet readableInputs = new IntOpenHashSet(inputChannels.size());

        for (int i = 0; i < inputChannels.size(); i++) {
          final ReadableFrameChannel channel = inputChannels.get(i);
          if (channel.isFinished() || channel.canRead()) {
            readableInputs.add(i);
          }
        }

        if (cancellationId != null) {
          // After this synchronized block, our thread may be interrupted by cancellations, because "cancel"
          // checks "runningProcessors".
          synchronized (lock) {
            if (cancelableProcessors.containsEntry(cancellationId, processor)) {
              runningProcessors.put(processor, Thread.currentThread());
            } else {
              // Processor has been canceled. We don't need to handle cleanup, because someone else did it.
              return Optional.empty();
            }
          }
        }

        boolean canceled = false;
        Either<Throwable, ReturnOrAwait<T>> retVal;

        try {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }

          retVal = Either.value(processor.runIncrementally(readableInputs));
        }
        catch (Throwable e) {
          retVal = Either.error(e);
        }
        finally {
          if (cancellationId != null) {
            // After this synchronized block, our thread will no longer be interrupted by cancellations,
            // because "cancel" checks "runningProcessors".
            synchronized (lock) {
              if (Thread.interrupted()) {
                // ignore: interrupt was meant for the processor, but came after the processor already exited.
              }

              runningProcessors.remove(processor);
              lock.notifyAll();

              if (!cancelableProcessors.containsEntry(cancellationId, processor)) {
                // Processor has been canceled by one of the "cancel" methods. They will handle cleanup.
                canceled = true;
              }
            }
          }
        }

        if (canceled) {
          return Optional.empty();
        } else {
          return Optional.of(retVal.valueOrThrow());
        }
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

      /**
       * Called when a processor succeeds.
       *
       * Runs the cleanup routine and sets the finished future to a particular value. If cleanup fails, sets the
       * finished future to an error.
       */
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

      /**
       * Called when a processor fails.
       *
       * Writes errors to all output channels, runs the cleanup routine, and sets the finished future to an error.
       */
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

      /**
       * Called when a processor exits via {@link #succeed} or {@link #fail}. Not called when a processor
       * is canceled.
       */
      void doProcessorCleanup() throws IOException
      {
        final boolean doCleanup;

        if (cancellationId != null) {
          synchronized (lock) {
            // Skip cleanup if the processor is no longer in cancelableProcessors. This means one of the "cancel"
            // methods is going to do the cleanup.
            doCleanup = cancelableProcessors.remove(cancellationId, processor);
          }
        } else {
          doCleanup = true;
        }

        if (doCleanup) {
          processor.cleanup();
        }
      }
    }

    final ExecutorRunnable runnable = new ExecutorRunnable();

    finished.addListener(
        () -> {
          logProcessorStatusString(processor, finished, null);

          // If the future was canceled, and the processor is cancelable, then cancel the processor too.
          if (finished.isCancelled() && cancellationId != null) {
            try {
              cancel(Collections.singleton(processor));
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            catch (Throwable e) {
              // No point throwing. Exceptions thrown in listeners don't go anywhere.
              log.noStackTrace().warn(e, "Exception encountered while canceling processor [%s]", processor);
            }
          }
        },
        Execs.directExecutor()
    );

    logProcessorStatusString(processor, finished, null);
    registerCancelableProcessor(processor, cancellationId);
    exec.submit(runnable);
    return finished;
  }

  /**
   * Cancels all processors associated with a given cancellationId. Waits for the processors to exit before
   * returning.
   */
  public void cancel(final String cancellationId) throws InterruptedException
  {
    Preconditions.checkNotNull(cancellationId, "cancellationId");

    final Set<ListenableFuture<?>> futuresToCancel;
    final Set<FrameProcessor<?>> processorsToCleanup;

    synchronized (lock) {
      futuresToCancel = cancelableFutures.removeAll(cancellationId);
      processorsToCleanup = cancelableProcessors.removeAll(cancellationId);
    }

    // Cancel all processors.
    cancel(processorsToCleanup);

    // Cancel all associated futures.
    for (final ListenableFuture<?> future : futuresToCancel) {
      future.cancel(true);
    }
  }

  <T, FutureType extends ListenableFuture<T>> FutureType registerCancelableFuture(
      final FutureType future,
      @Nullable final String cancellationId
  )
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancelableFutures.put(cancellationId, future);
        future.addListener(
            () -> {
              synchronized (lock) {
                cancelableFutures.remove(cancellationId, future);
              }
            },
            Execs.directExecutor()
        );
      }
    }

    return future;
  }

  /**
   * Cancels a given processor associated with a given cancellationId. Waits for the processor to exit before
   * returning.
   */
  private void cancel(final Set<FrameProcessor<?>> processorsToCleanup)
      throws InterruptedException
  {
    synchronized (lock) {
      for (final FrameProcessor<?> processor : processorsToCleanup) {
        final Thread processorThread = runningProcessors.get(processor);
        if (processorThread != null) {
          // Interrupt the thread running the processor. Do this under lock, because we want to make sure we don't
          // interrupt the thread shortly *before* or *after* it runs the processor.
          processorThread.interrupt();
        }
      }

      // Wait for all running processors to stop running. Then clean them up outside the critical section.
      while (processorsToCleanup.stream().anyMatch(runningProcessors::containsKey)) {
        lock.wait();
      }
    }

    // Now processorsToCleanup are not running, also won't run again, because we removed them from cancelableProcessors.
    // (runProcessorNow checks for that.) Run their cleanup routines outside the critical section.
    try (final Closer closer = Closer.create()) {
      for (final FrameProcessor<?> processor : processorsToCleanup) {
        closer.register(processor::cleanup);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> void registerCancelableProcessor(final FrameProcessor<T> processor, @Nullable final String cancellationId)
  {
    if (cancellationId != null) {
      synchronized (lock) {
        cancelableProcessors.put(cancellationId, processor);
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
