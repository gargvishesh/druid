/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameStorageAdapter;
import io.imply.druid.talaria.util.TalariaFutureUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class FrameProcessors
{
  private static final Logger log = new Logger(FrameProcessors.class);

  private FrameProcessors()
  {
    // No instantiation.
  }

  /**
   * Runs a series of processors until using a given executor service, with a given maximum number of outstanding
   * processors, and returns a future that resolves when execution is complete. The tasks submitted to the executor
   * will not block.
   *
   * The return value is an accumulation using the provided "accumulateFn". This function is applied in a critical
   * section, so it should be something that executes quickly. It will get null for the first accumulated value.
   *
   * If there are no processors at all to run, this method will return an immediate future containing "initialResult".
   */
  public static <T, ResultType> ListenableFuture<ResultType> runAllFully(
      final Sequence<? extends FrameProcessor<T>> processors,
      final FrameProcessorExecutor exec,
      final ResultType initialResult,
      final BiFunction<ResultType, T, ResultType> accumulateFn,
      final int maxOutstandingProcessors,
      final Bouncer bouncer,
      @Nullable final String cancellationId
  )
  {
    final Yielder<? extends FrameProcessor<T>> processorYielder = Yielders.each(processors);

    if (processorYielder.isDone()) {
      return Futures.immediateFuture(initialResult);
    }

    // This single RunAllFullyRunnable will be submitted to the executor "maxOutstandingProcessors" times.
    final RunAllFullyRunnable<T, ResultType> runnable = new RunAllFullyRunnable<>(
        processorYielder,
        exec,
        initialResult,
        accumulateFn,
        maxOutstandingProcessors,
        bouncer,
        cancellationId
    );

    runnable.finished.addListener(
        () -> {
          if (runnable.finished.isCancelled()) {
            try {
              runnable.cleanupIfNoMoreProcessors();
            }
            catch (Throwable e) {
              // No point throwing. Exceptions thrown in listeners don't go anywhere.
              log.noStackTrace().warn(e, "Exception encountered while cleaning up canceled runAllFully execution");
            }
          }
        },
        Execs.directExecutor()
    );

    for (int i = 0; i < maxOutstandingProcessors; i++) {
      exec.getExecutorService().submit(runnable);
    }

    return runnable.finished;
  }

  public static <T> FrameProcessor<T> withBaggage(final FrameProcessor<T> processor, final Closeable baggage)
  {
    class FrameProcessorWithBaggage implements FrameProcessor<T>
    {
      final AtomicBoolean cleanedUp = new AtomicBoolean();

      @Override
      public List<ReadableFrameChannel> inputChannels()
      {
        return processor.inputChannels();
      }

      @Override
      public List<WritableFrameChannel> outputChannels()
      {
        return processor.outputChannels();
      }

      @Override
      public ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws IOException
      {
        return processor.runIncrementally(readableInputs);
      }

      @Override
      public void cleanup() throws IOException
      {
        if (cleanedUp.compareAndSet(false, true)) {
          //noinspection EmptyTryBlock
          try (Closeable ignore1 = baggage;
               Closeable ignore2 = processor::cleanup) {
            // piggy-back try-with-resources semantics
          }
        }
      }

      @Override
      public String toString()
      {
        return processor + " (with baggage)";
      }
    }

    return new FrameProcessorWithBaggage();
  }

  public static Cursor makeCursor(final Frame frame, final FrameReader frameReader)
  {
    // Safe to never close the Sequence that the Cursor comes from, because it does not do anything when it is closed.
    // Refer to FrameStorageAdapter#makeCursors.

    return Yielders.each(
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null)
    ).get();
  }

  /**
   * Helper method for implementing {@link FrameProcessor#cleanup()}.
   *
   * The objects are closed in the order provided.
   */
  public static void closeAll(
      final List<ReadableFrameChannel> readableFrameChannels,
      final List<WritableFrameChannel> writableFrameChannels,
      final Closeable... otherCloseables
  ) throws IOException
  {
    final Closer closer = Closer.create();

    // Add everything to the Closer in reverse order, because the Closer closes in reverse order.

    for (Closeable closeable : Lists.reverse(Arrays.asList(otherCloseables))) {
      if (closeable != null) {
        closer.register(closeable);
      }
    }

    for (WritableFrameChannel channel : Lists.reverse(writableFrameChannels)) {
      closer.register(channel::doneWriting);
    }

    for (ReadableFrameChannel channel : Lists.reverse(readableFrameChannels)) {
      closer.register(channel::doneReading);
    }

    closer.close();
  }

  private static class RunAllFullyRunnable<T, ResultType> implements Runnable
  {
    private final FrameProcessorExecutor exec;
    private final ResultType initialResult;
    private final BiFunction<ResultType, T, ResultType> accumulateFn;
    private final int maxOutstandingProcessors;
    private final Bouncer bouncer;
    @Nullable
    private final String cancellationId;
    private final SettableFuture<ResultType> finished;

    private final Object runAllFullyLock = new Object();

    @GuardedBy("runAllFullyLock")
    Yielder<? extends FrameProcessor<T>> processorYielder;

    @GuardedBy("runAllFullyLock")
    ResultType currentResult = null;

    @GuardedBy("runAllFullyLock")
    boolean seenFirstResult = false;

    @GuardedBy("runAllFullyLock")
    int outstandingProcessors = 0;

    @Nullable // nulled out by cleanup()
    @GuardedBy("runAllFullyLock")
    Queue<Bouncer.Ticket> bouncerQueue = new ArrayDeque<>();

    private RunAllFullyRunnable(
        final Yielder<? extends FrameProcessor<T>> processorYielder,
        final FrameProcessorExecutor exec,
        final ResultType initialResult,
        final BiFunction<ResultType, T, ResultType> accumulateFn,
        final int maxOutstandingProcessors,
        final Bouncer bouncer,
        @Nullable final String cancellationId
    )
    {
      this.processorYielder = processorYielder;
      this.exec = exec;
      this.initialResult = initialResult;
      this.accumulateFn = accumulateFn;
      this.maxOutstandingProcessors = maxOutstandingProcessors;
      this.bouncer = bouncer;
      this.cancellationId = cancellationId;
      this.finished = exec.registerCancelableFuture(SettableFuture.create(), cancellationId);
    }

    @Override
    public void run()
    {
      final FrameProcessor<T> nextProcessor;
      Bouncer.Ticket nextTicket = null;

      synchronized (runAllFullyLock) {
        if (finished.isDone()) {
          cleanupIfNoMoreProcessors();
          return;
        } else if (!processorYielder.isDone()) {
          assert bouncerQueue != null;

          try {
            final Bouncer.Ticket ticketFromQueue = bouncerQueue.poll();

            if (ticketFromQueue != null) {
              nextTicket = ticketFromQueue;
            } else {
              final ListenableFuture<Bouncer.Ticket> ticketFuture =
                  exec.registerCancelableFuture(bouncer.ticket(), cancellationId);

              if (ticketFuture.isDone() && !ticketFuture.isCancelled()) {
                nextTicket = TalariaFutureUtils.getUncheckedImmediately(ticketFuture);
              } else {
                ticketFuture.addListener(
                    () -> {
                      if (!ticketFuture.isCancelled()) {
                        // No need to check for exception; bouncer tickets cannot have exceptions.
                        final Bouncer.Ticket ticket = TalariaFutureUtils.getUncheckedImmediately(ticketFuture);

                        synchronized (runAllFullyLock) {
                          if (finished.isDone()) {
                            ticket.giveBack();
                            return;
                          } else {
                            bouncerQueue.add(ticket);
                          }
                        }
                        exec.getExecutorService().submit(RunAllFullyRunnable.this);
                      }
                    },
                    Execs.directExecutor()
                );

                return;
              }
            }

            assert outstandingProcessors < maxOutstandingProcessors;
            nextProcessor = processorYielder.get();
            processorYielder = processorYielder.next(null);
            outstandingProcessors++;
          }
          catch (Throwable e) {
            if (nextTicket != null) {
              nextTicket.giveBack();
            }
            finished.setException(e);
            cleanupIfNoMoreProcessors();
            return;
          }
        } else {
          return;
        }

        assert nextTicket != null;
        final ListenableFuture<T> future = exec.runFully(
            FrameProcessors.withBaggage(nextProcessor, nextTicket::giveBack),
            cancellationId
        );

        Futures.addCallback(
            future,
            new FutureCallback<T>()
            {
              @Override
              public void onSuccess(T result)
              {
                final boolean isDone;
                ResultType retVal = null;

                try {
                  synchronized (runAllFullyLock) {
                    outstandingProcessors--;

                    if (!seenFirstResult) {
                      currentResult = accumulateFn.apply(initialResult, result);
                      seenFirstResult = true;
                    } else {
                      currentResult = accumulateFn.apply(currentResult, result);
                    }

                    isDone = outstandingProcessors == 0 && processorYielder.isDone();

                    if (isDone) {
                      retVal = currentResult;
                    }
                  }
                }
                catch (Throwable e) {
                  finished.setException(e);
                  cleanupIfNoMoreProcessors();
                  return;
                }

                if (isDone) {
                  finished.set(retVal);
                  cleanupIfNoMoreProcessors();
                } else {
                  // Not finished; run again.
                  exec.getExecutorService().submit(RunAllFullyRunnable.this);
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                finished.setException(t);

                synchronized (runAllFullyLock) {
                  outstandingProcessors--;
                  cleanupIfNoMoreProcessors();
                }
              }
            }
        );
      }
    }

    /**
     * Run {@link #cleanup()} if no more processors will be launched.
     */
    private void cleanupIfNoMoreProcessors()
    {
      synchronized (runAllFullyLock) {
        if (outstandingProcessors == 0 && finished.isDone()) {
          cleanup();
        }
      }
    }

    /**
     * Cleanup work that must happen after everything has calmed down.
     */
    @GuardedBy("runAllFullyLock")
    private void cleanup()
    {
      assert finished.isDone();

      try {
        if (bouncerQueue != null) {
          // Drain ticket queue, return everything.

          Bouncer.Ticket ticket;
          while ((ticket = bouncerQueue.poll()) != null) {
            ticket.giveBack();
          }

          bouncerQueue = null;
        }

        if (processorYielder != null) {
          processorYielder.close();
          processorYielder = null;
        }
      }
      catch (Throwable e) {
        // No point throwing, since our caller is just a future callback.
        log.noStackTrace().warn(e, "Exception encountered while cleaning up from runAllFully");
      }
    }
  }
}
