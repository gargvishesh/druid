/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
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
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StagePartition;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class BaseLeafFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private static final Logger log = new Logger(BaseLeafFrameProcessorFactory.class);

  // Input specs for the base datasource.
  private final List<QueryWorkerInputSpec> baseInputSpecs;

  protected BaseLeafFrameProcessorFactory(final List<QueryWorkerInputSpec> baseInputSpecs)
  {
    this.baseInputSpecs = Preconditions.checkNotNull(baseInputSpecs, "baseInputSpecs");
  }

  @Override
  public ProcessorsAndChannels<FrameProcessor<Long>, Long> makeProcessors(
      int workerNumber,
      @Nullable Object extra,
      InputChannels inputChannels,
      OutputChannelFactory outputChannelFactory,
      StageDefinition stageDefinition,
      ClusterBy clusterBy,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      TalariaCounters talariaCounters
  ) throws IOException
  {
    final QueryWorkerInputSpec inputSpec = baseInputSpecs.get(workerNumber);
    final int totalProcessors = QueryWorkerUtils.numProcessors(inputSpec, inputChannels);

    if (totalProcessors == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final int outstandingProcessors;

    if (inputSpec.type() == QueryWorkerInputType.EXTERNAL
        && inputSpec.getInputFormat().getClass().getName().contains("Parquet")) {
      // Hack alert: workaround for memory use in ParquetFileReader: https://implydata.atlassian.net/browse/IMPLY-17932
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
      final OutputChannel outputChannel = outputChannelFactory.openChannel(FrameWithPartition.NO_PARTITION);
      outputChannels.add(outputChannel);
      channelQueueRef.get().add(outputChannel.getWritableChannel());
      allocatorQueueRef.get().add(outputChannel.getFrameMemoryAllocator());
    }

    final DataSegmentProvider dataSegmentProvider = frameContext.dataSegmentProvider();
    final File temporaryDirectory = new File(frameContext.tempDir(), "input-source-" + UUID.randomUUID());

    final Sequence<QueryWorkerInput> processorBaseInputs =
        Sequences.simple(
            () ->
                QueryWorkerUtils.inputIterator(
                    workerNumber,
                    inputSpec,
                    stageDefinition,
                    inputChannels,
                    dataSegmentProvider,
                    temporaryDirectory,
                    talariaCounters
                )
        );

    final Sequence<FrameProcessor<Long>> processors = processorBaseInputs.map(
        processorBaseInput -> {
          // TODO(gianm): this is wasteful: we're opening channels and rebuilding broadcast tables for _every processor_
          final Pair<Int2ObjectMap<ReadableFrameChannel>, Int2ObjectMap<FrameReader>> sideChannels =
              openSideChannels(inputChannels, inputSpec);
          final TalariaCounters.ChannelCounters channelCounters;

          return makeProcessor(
              processorBaseInput,
              sideChannels.lhs,
              sideChannels.rhs,
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
              clusterBy,
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

    return new ProcessorsAndChannels<>(processors, OutputChannels.wrap(outputChannels));
  }

  private static Pair<Int2ObjectMap<ReadableFrameChannel>, Int2ObjectMap<FrameReader>> openSideChannels(
      final InputChannels inputChannels,
      final QueryWorkerInputSpec baseInputSpec
  )
  {
    final IntSet sideStageNumbers = new IntOpenHashSet();
    final Int2ObjectMap<FrameReader> sideChannelReaders = new Int2ObjectOpenHashMap<>();

    for (final StagePartition stagePartition : inputChannels.getStagePartitions()) {
      final int stageNumber = stagePartition.getStageId().getStageNumber();

      if (baseInputSpec.type() != QueryWorkerInputType.SUBQUERY || baseInputSpec.getStageNumber() != stageNumber) {
        sideStageNumbers.add(stageNumber);
        sideChannelReaders.put(stageNumber, inputChannels.getFrameReader(stagePartition));
      }
    }

    final Int2ObjectMap<ReadableFrameChannel> sideChannels = new Int2ObjectOpenHashMap<>();

    for (final int stageNumber : sideStageNumbers) {
      sideChannels.put(
          stageNumber,
          ReadableConcatFrameChannel.open(
              Iterators.transform(
                  Iterators.filter(
                      inputChannels.getStagePartitions().iterator(),
                      stagePartition -> stagePartition.getStageId().getStageNumber() == stageNumber
                  ),
                  stagePartition -> {
                    try {
                      return inputChannels.openChannel(stagePartition);
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              )
          )
      );
    }

    return Pair.of(sideChannels, sideChannelReaders);
  }

  protected abstract FrameProcessor<Long> makeProcessor(
      QueryWorkerInput baseInput,
      Int2ObjectMap<ReadableFrameChannel> sideChannels,
      Int2ObjectMap<FrameReader> sideChannelReaders,
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

  @Override
  public int inputFileCount()
  {
    int inputFiles = 0;
    for (QueryWorkerInputSpec inputSpec : baseInputSpecs) {
      if (inputSpec.getInputSources() != null && inputSpec.type().equals(QueryWorkerInputType.EXTERNAL)) {
        inputFiles += inputSpec.getInputSources().stream().filter(QueryWorkerUtils::isFileBasedInputSource).count();
      } else if (inputSpec.type().equals(QueryWorkerInputType.TABLE)) {
        inputFiles += inputSpec.getSegments().size();
      }
    }
    return inputFiles;
  }
}
