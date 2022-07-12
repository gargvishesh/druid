/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.collect.Iterators;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.ReadableConcatFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.input.ExternalInputSlice;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.input.InputSlices;
import io.imply.druid.talaria.input.ReadableInput;
import io.imply.druid.talaria.input.ReadableInputs;
import io.imply.druid.talaria.input.StageInputSlice;
import io.imply.druid.talaria.kernel.StageDefinition;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class BaseLeafFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private static final Logger log = new Logger(BaseLeafFrameProcessorFactory.class);

  @Override
  public ProcessorsAndChannels<FrameProcessor<Long>, Long> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  ) throws IOException
  {
    // BaseLeafFrameProcessorFactory is used for native Druid queries, where the following input cases can happen:
    //   1) Union datasources: N nonbroadcast inputs, which are are treated as one big input
    //   2) Join datasources: one nonbroadcast input, N broadcast inputs
    //   3) All other datasources: single input

    final int totalProcessors = InputSlices.getNumNonBroadcastReadableInputs(
        inputSlices,
        inputSliceReader,
        stageDefinition.getBroadcastInputNumbers()
    );

    if (totalProcessors == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final int outstandingProcessors;

    if (hasParquet(inputSlices)) {
      // Hack alert: workaround for memory use in ParquetFileReader, which loads up an entire row group into memory as
      // part of its normal operation. Row groups can be quite large (like, 1GB large) so this is a major source of
      // unaccounted-for memory use during ingestion and query of external data. Work around this by only running
      // a single processor at once.
      outstandingProcessors = 1;
    } else {
      outstandingProcessors = Math.min(totalProcessors, maxOutstandingProcessors);
    }

    final AtomicReference<Queue<MemoryAllocator>> allocatorQueueRef =
        new AtomicReference<>(new ArrayDeque<>(outstandingProcessors));
    final AtomicReference<Queue<WritableFrameChannel>> channelQueueRef =
        new AtomicReference<>(new ArrayDeque<>(outstandingProcessors));
    final List<OutputChannel> outputChannels = new ArrayList<>(outstandingProcessors);

    for (int i = 0; i < outstandingProcessors; i++) {
      final OutputChannel outputChannel = outputChannelFactory.openChannel(0 /* Partition number doesn't matter */);
      outputChannels.add(outputChannel);
      channelQueueRef.get().add(outputChannel.getWritableChannel());
      allocatorQueueRef.get().add(outputChannel.getFrameMemoryAllocator());
    }

    // Read all base inputs in separate processors, one per processor.
    final Sequence<ReadableInput> processorBaseInputs = readBaseInputs(
        stageDefinition,
        inputSlices,
        inputSliceReader,
        counters,
        warningPublisher
    );

    final Sequence<FrameProcessor<Long>> processors = processorBaseInputs.map(
        processorBaseInput -> {
          // TODO(gianm): this is wasteful: we're opening channels and rebuilding broadcast tables for _every processor_
          final Int2ObjectMap<ReadableInput> sideChannels =
              readBroadcastInputs(stageDefinition, inputSlices, inputSliceReader, counters, warningPublisher);

          return makeProcessor(
              processorBaseInput,
              sideChannels,
              makeLazyResourceHolder(
                  channelQueueRef,
                  channel -> {
                    try {
                      channel.doneWriting();
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              ),
              makeLazyResourceHolder(allocatorQueueRef, ignored -> {
              }),
              stageDefinition.getSignature(),
              stageDefinition.getClusterBy(),
              frameContext
          );
        }
    ).withBaggage(
        () -> {
          final Queue<WritableFrameChannel> channelQueue;
          synchronized (channelQueueRef) {
            // Set to null so any channels returned by outstanding workers are immediately closed.
            channelQueue = channelQueueRef.getAndSet(null);
          }

          WritableFrameChannel c;
          while ((c = channelQueue.poll()) != null) {
            try {
              c.doneWriting();
            }
            catch (Throwable e) {
              log.warn(e, "Error encountered while closing channel for [%s]", this);
            }
          }
        }
    );

    return new ProcessorsAndChannels<>(processors, OutputChannels.wrapReadOnly(outputChannels));
  }

  /**
   * Read base inputs, where "base" is meant in the same sense as in
   * {@link org.apache.druid.query.planning.DataSourceAnalysis}: the primary datasource that drives query processing.
   */
  private static Sequence<ReadableInput> readBaseInputs(
      final StageDefinition stageDef,
      final List<InputSlice> inputSlices,
      final InputSliceReader inputSliceReader,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final List<Sequence<ReadableInput>> sequences = new ArrayList<>();

    for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
      if (!stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        final int i = inputNumber;
        final Sequence<ReadableInput> sequence =
            Sequences.simple(inputSliceReader.attach(i, inputSlices.get(i), counters, warningPublisher));
        sequences.add(sequence);
      }
    }

    return Sequences.concat(sequences);
  }

  /**
   * Reads all broadcast inputs, which must be {@link StageInputSlice}. The execution framework supports broadcasting
   * other types of inputs, but QueryKit does not use them at this time.
   *
   * Returns a map of input number -> channel containing all data for that input number.
   */
  private static Int2ObjectMap<ReadableInput> readBroadcastInputs(
      final StageDefinition stageDef,
      final List<InputSlice> inputSlices,
      final InputSliceReader inputSliceReader,
      final CounterTracker counterTracker,
      final Consumer<Throwable> warningPublisher
  )
  {
    final Int2ObjectMap<ReadableInput> broadcastInputs = new Int2ObjectAVLTreeMap<>();

    try {
      for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
        if (stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
          // QueryKit only uses StageInputSlice at this time.
          final StageInputSlice slice = (StageInputSlice) inputSlices.get(inputNumber);
          final ReadableInputs readableInputs =
              inputSliceReader.attach(inputNumber, slice, counterTracker, warningPublisher);

          if (!readableInputs.hasChannels()) {
            // QueryKit limitation: broadcast inputs must be channels.
            throw new ISE("Broadcast inputs must be channels");
          }

          final ReadableFrameChannel channel = ReadableConcatFrameChannel.open(
              Iterators.transform(readableInputs.iterator(), ReadableInput::getChannel)
          );
          broadcastInputs.put(inputNumber, ReadableInput.channel(channel, readableInputs.frameReader(), null));
        }
      }

      return broadcastInputs;
    }
    catch (Throwable e) {
      // Close any already-opened channels.
      try {
        broadcastInputs.values().forEach(input -> input.getChannel().doneReading());
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      throw e;
    }
  }

  protected abstract FrameProcessor<Long> makeProcessor(
      ReadableInput baseInput,
      Int2ObjectMap<ReadableInput> sideChannels,
      ResourceHolder<WritableFrameChannel> outputChannelSupplier,
      ResourceHolder<MemoryAllocator> allocatorSupplier,
      RowSignature signature,
      ClusterBy clusterBy,
      FrameContext providerThingy
  );

  private static <T> ResourceHolder<T> makeLazyResourceHolder(
      final AtomicReference<Queue<T>> queueRef,
      final Consumer<T> backupCloser
  )
  {
    return new LazyResourceHolder<>(
        () -> {
          final T resource;

          synchronized (queueRef) {
            resource = queueRef.get().poll();
          }

          return Pair.of(
              resource,
              () -> {
                synchronized (queueRef) {
                  final Queue<T> queue = queueRef.get();
                  if (queue != null) {
                    queue.add(resource);
                    return;
                  }
                }

                // Queue was null
                backupCloser.accept(resource);
              }
          );
        }
    );
  }

  private static boolean hasParquet(final List<InputSlice> slices)
  {
    return slices.stream().anyMatch(
        slice ->
            slice instanceof ExternalInputSlice
            && ((ExternalInputSlice) slice).getInputFormat().getClass().getName().contains("Parquet")
    );
  }
}
