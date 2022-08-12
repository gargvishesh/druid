/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.talaria.counters.CounterNames;
import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableNilFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import io.imply.druid.talaria.frame.processor.BlockingQueueOutputChannelFactory;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.frame.processor.FileOutputChannelFactory;
import io.imply.druid.talaria.frame.processor.FrameChannelMuxer;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessorExecutor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.frame.processor.SuperSorter;
import io.imply.druid.talaria.indexing.CountingOutputChannelFactory;
import io.imply.druid.talaria.indexing.InputChannelFactory;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.KeyStatisticsCollectionProcessor;
import io.imply.druid.talaria.indexing.SuperSorterProgressTracker;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportLimiterPublisher;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportSimplePublisher;
import io.imply.druid.talaria.input.ExternalInputSlice;
import io.imply.druid.talaria.input.ExternalInputSliceReader;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.input.InputSlices;
import io.imply.druid.talaria.input.MapInputSliceReader;
import io.imply.druid.talaria.input.NilInputSlice;
import io.imply.druid.talaria.input.NilInputSliceReader;
import io.imply.druid.talaria.input.SegmentsInputSlice;
import io.imply.druid.talaria.input.SegmentsInputSliceReader;
import io.imply.druid.talaria.input.StageInputSlice;
import io.imply.druid.talaria.input.StageInputSliceReader;
import io.imply.druid.talaria.kernel.FrameProcessorFactory;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.kernel.worker.WorkerStageKernel;
import io.imply.druid.talaria.kernel.worker.WorkerStagePhase;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import io.imply.druid.talaria.shuffle.DurableStorageInputChannelFactory;
import io.imply.druid.talaria.shuffle.DurableStorageOutputChannelFactory;
import io.imply.druid.talaria.shuffle.WorkerInputChannelFactory;
import io.imply.druid.talaria.util.DecoratedExecutorService;
import io.imply.druid.talaria.util.TalariaContext;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.PrioritizedCallable;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class WorkerImpl implements Worker
{
  private static final Logger log = new Logger(WorkerImpl.class);

  private final TalariaWorkerTask task;
  private final WorkerContext context;

  private final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue = new LinkedBlockingDeque<>();
  private final ConcurrentHashMap<StageId, ConcurrentHashMap<Integer, ReadableFrameChannel>> stageOutputs = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<StageId, CounterTracker> stageCounters = new ConcurrentHashMap<>();
  private final boolean durableStageStorageEnabled;

  private volatile DruidNode selfDruidNode;
  private volatile LeaderClient leaderClient;
  private volatile WorkerClient workerClient;
  private volatile Bouncer processorBouncer;
  private volatile boolean leaderAlive = true;

  public WorkerImpl(TalariaWorkerTask task, WorkerContext context)
  {
    this.task = task;
    this.context = context;
    this.durableStageStorageEnabled = TalariaContext.isDurableStorageEnabled(task.getContext());
  }

  @Override
  public String id()
  {
    return task.getId();
  }

  @Override
  public TalariaWorkerTask task()
  {
    return task;
  }

  @Override
  public TaskStatus run() throws Exception
  {
    try (final Closer closer = Closer.create()) {
      Optional<MSQErrorReport> maybeErrorReport;

      try {
        maybeErrorReport = runTask(closer);
      }
      catch (Throwable e) {
        maybeErrorReport = Optional.of(
            MSQErrorReport.fromException(id(), TalariaTasks.getHostFromSelfNode(selfDruidNode), null, e)
        );
      }

      if (maybeErrorReport.isPresent()) {
        final MSQErrorReport errorReport = maybeErrorReport.get();
        final String errorLogMessage = TalariaTasks.errorReportToLogMessage(errorReport);
        log.warn(errorLogMessage);

        closer.register(() -> {
          if (leaderAlive && leaderClient != null && selfDruidNode != null) {
            leaderClient.postWorkerError(id(), errorReport);
          }
        });

        return TaskStatus.failure(id(), errorReport.getFault().getCodeWithMessage());
      } else {
        return TaskStatus.success(id());
      }
    }
  }

  /**
   * Runs worker logic. Returns an empty Optional on success. On failure, returns an error report for errors that
   * happened in other threads; throws exceptions for errors that happened in the main worker loop.
   */
  public Optional<MSQErrorReport> runTask(final Closer closer) throws Exception
  {
    this.selfDruidNode = context.selfNode();
    this.leaderClient = context.makeLeaderClient(task.getControllerTaskId());
    closer.register(leaderClient::close);
    context.registerWorker(this, closer); // Uses leaderClient, so must be called after leaderClient is initialized
    this.workerClient = new ExceptionWrappingWorkerClient(context.makeWorkerClient());
    closer.register(workerClient::close);
    this.processorBouncer = context.processorBouncer();

    final KernelHolder kernelHolder = new KernelHolder();
    final String cancellationId = id();

    final FrameProcessorExecutor workerExec = new FrameProcessorExecutor(makeProcessingPool());

    // Delete all the stage outputs
    closer.register(() -> {
      for (final StageId stageId : stageOutputs.keySet()) {
        cleanStageOutput(stageId);
      }
    });

    // Close stage output processors and running futures (if present)
    closer.register(() -> {
      try {
        workerExec.cancel(cancellationId);
      }
      catch (InterruptedException e) {
        // Strange that cancelation would itself be interrupted. Throw an exception, since this is unexpected.
        throw new RuntimeException(e);
      }
    });

    final TalariaWarningReportPublisher talariaWarningReportPublisher = new TalariaWarningReportLimiterPublisher(
        new TalariaWarningReportSimplePublisher(
            id(),
            leaderClient,
            id(),
            TalariaTasks.getHostFromSelfNode(selfDruidNode)
        )
    );

    closer.register(talariaWarningReportPublisher);

    final Map<StageId, SettableFuture<ClusterByPartitions>> partitionBoundariesFutureMap = new HashMap<>();

    final Map<StageId, FrameContext> stageFrameContexts = new HashMap<>();

    while (!kernelHolder.isDone()) {
      boolean didSomething = false;

      for (final WorkerStageKernel kernel : kernelHolder.getStageKernelMap().values()) {
        final StageDefinition stageDefinition = kernel.getStageDefinition();

        if (kernel.getPhase() == WorkerStagePhase.NEW) {
          log.debug("New work order: %s", context.jsonMapper().writeValueAsString(kernel.getWorkOrder()));

          // Create separate inputChannelFactory per stage, because the list of tasks can grow between stages, and
          // so we need to avoid the memoization in baseInputChannelFactory.
          final InputChannelFactory inputChannelFactory = makeBaseInputChannelFactory(closer);

          // Compute memory parameters for all stages, even ones that haven't been assigned yet, so we can fail-fast
          // if some won't work. (We expect that all stages will get assigned to the same pool of workers.)
          for (final StageDefinition stageDef : kernel.getWorkOrder().getQueryDefinition().getStageDefinitions()) {
            stageFrameContexts.computeIfAbsent(
                stageDef.getId(),
                stageId -> context.frameContext(
                    kernel.getWorkOrder().getQueryDefinition(),
                    stageId.getStageNumber()
                )
            );
          }

          // Start working on this stage immediately.
          kernel.startReading();
          final SettableFuture<ClusterByPartitions> partitionBoundariesFuture =
              startWorkOrder(
                  kernel,
                  inputChannelFactory,
                  stageCounters.computeIfAbsent(stageDefinition.getId(), ignored -> new CounterTracker()),
                  workerExec,
                  cancellationId,
                  context.threadCount(),
                  stageFrameContexts.get(stageDefinition.getId()),
                  talariaWarningReportPublisher
              );

          if (partitionBoundariesFuture != null) {
            if (partitionBoundariesFutureMap.put(stageDefinition.getId(), partitionBoundariesFuture) != null) {
              throw new ISE("Work order collision for stage [%s]", stageDefinition.getId());
            }
          }

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        if (kernel.getPhase() == WorkerStagePhase.READING_INPUT && kernel.hasResultKeyStatisticsSnapshot()) {
          if (leaderAlive) {
            leaderClient.postKeyStatistics(
                stageDefinition.getId(),
                kernel.getWorkOrder().getWorkerNumber(),
                kernel.getResultKeyStatisticsSnapshot()
            );
          }
          kernel.startPreshuffleWaitingForResultPartitionBoundaries();

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        logKernelStatus(kernelHolder.getStageKernelMap().values());
        if (kernel.getPhase() == WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES
            && kernel.hasResultPartitionBoundaries()) {
          partitionBoundariesFutureMap.get(stageDefinition.getId()).set(kernel.getResultPartitionBoundaries());
          kernel.startPreshuffleWritingOutput();

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        if (kernel.getPhase() == WorkerStagePhase.RESULTS_READY
            && kernel.addPostedResultsComplete(Pair.of(stageDefinition.getId(), kernel.getWorkOrder().getWorkerNumber()))) {
          if (leaderAlive) {
            leaderClient.postResultsComplete(
                stageDefinition.getId(),
                kernel.getWorkOrder().getWorkerNumber(),
                kernel.getResultObject()
            );
          }
        }

        if (kernel.getPhase() == WorkerStagePhase.FAILED) {
          // Better than throwing an exception, because we can include the stage number.
          return Optional.of(
              MSQErrorReport.fromException(
                  id(),
                  TalariaTasks.getHostFromSelfNode(selfDruidNode),
                  stageDefinition.getId().getStageNumber(),
                  kernel.getException()
              )
          );
        }
      }

      if (!didSomething && !kernelHolder.isDone()) {
        Consumer<KernelHolder> nextCommand;

        do {
          postCountersToController();
        } while ((nextCommand = kernelManipulationQueue.poll(5, TimeUnit.SECONDS)) == null);

        nextCommand.accept(kernelHolder);
        logKernelStatus(kernelHolder.getStageKernelMap().values());
      }
    }

    // Empty means success.
    return Optional.empty();
  }

  @Override
  public void stopGracefully()
  {
    kernelManipulationQueue.add(
        kernel -> {
          // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
          throw new TalariaException(CanceledFault.INSTANCE);
        }
    );
  }

  @Override
  public void leaderFailed()
  {
    leaderAlive = false;
    stopGracefully();
  }

  @Override
  public InputStream readChannel(
      final String queryId,
      final int stageNumber,
      final int partitionNumber,
      final long offset
  ) throws IOException
  {
    final StageId stageId = new StageId(queryId, stageNumber);
    final StagePartition stagePartition = new StagePartition(stageId, partitionNumber);
    final ConcurrentHashMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.get(stageId);

    if (partitionOutputsForStage == null) {
      return null;
    }
    final ReadableFrameChannel channel = partitionOutputsForStage.get(partitionNumber);

    if (channel == null) {
      return null;
    }

    if (channel instanceof ReadableNilFrameChannel) {
      // Build an empty frame file.
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      FrameFileWriter.open(Channels.newChannel(baos)).close();

      final ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());

      //noinspection ResultOfMethodCallIgnored: OK to ignore since "skip" always works for ByteArrayInputStream.
      in.skip(offset);

      return in;
    } else if (channel instanceof ReadableFileFrameChannel) {
      // Close frameFile once we've returned an input stream: no need to retain a reference to the mmap after that,
      // since we aren't using it.
      try (final FrameFile frameFile = ((ReadableFileFrameChannel) channel).newFrameFileReference()) {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(frameFile.file(), "r");

        if (offset >= randomAccessFile.length()) {
          randomAccessFile.close();
          return new ByteArrayInputStream(ByteArrays.EMPTY_ARRAY);
        } else {
          randomAccessFile.seek(offset);
          return Channels.newInputStream(randomAccessFile.getChannel());
        }
      }
    } else {
      String errorMsg = StringUtils.format(
          "Returned server error to client because channel for [%s] is not nil or file-based (class = %s)",
          stagePartition,
          channel.getClass().getName()
      );
      log.error(StringUtils.encodeForFormat(errorMsg));

      throw new IOException(errorMsg);
    }
  }

  @Override
  public void postWorkOrder(final WorkOrder workOrder)
  {
    if (task.getWorkerNumber() != workOrder.getWorkerNumber()) {
      throw new ISE("Worker number mismatch: expected [%d]", task.getWorkerNumber());
    }

    kernelManipulationQueue.add(
        kernelHolder ->
            kernelHolder.getStageKernelMap().computeIfAbsent(
                workOrder.getStageDefinition().getId(),
                ignored -> WorkerStageKernel.create(workOrder)
            )
    );
  }

  @Override
  public boolean postResultPartitionBoundaries(
      final ClusterByPartitions stagePartitionBoundaries,
      final String queryId,
      final int stageNumber
  )
  {
    final StageId stageId = new StageId(queryId, stageNumber);

    kernelManipulationQueue.add(
        kernelHolder -> {
          final WorkerStageKernel stageKernel = kernelHolder.getStageKernelMap().get(stageId);

          // Ignore the update if we don't have a kernel for this stage.
          if (stageKernel != null) {
            stageKernel.setResultPartitionBoundaries(stagePartitionBoundaries);
          } else {
            log.warn("Ignored result partition boundaries call for unknown stage [%s]", stageId);
          }
        }
    );
    return true;
  }

  @Override
  public void postCleanupStage(final StageId stageId)
  {
    log.info("Cleanup order for stage: [%s] received", stageId);
    kernelManipulationQueue.add(
        holder -> {
          cleanStageOutput(stageId);
          // Mark the stage as FINISHED
          holder.getStageKernelMap().get(stageId).setStageFinished();
        }
    );
  }

  @Override
  public void postFinish()
  {
    kernelManipulationQueue.add(KernelHolder::setDone);
  }

  @Override
  public CounterSnapshotsTree getCounters()
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();

    for (final Map.Entry<StageId, CounterTracker> entry : stageCounters.entrySet()) {
      retVal.put(entry.getKey().getStageNumber(), task().getWorkerNumber(), entry.getValue().snapshot());
    }

    return retVal;
  }

  private InputChannelFactory makeBaseInputChannelFactory(final Closer closer)
  {
    final Supplier<List<String>> workerTaskList = Suppliers.memoize(
        () -> {
          try {
            return leaderClient.getTaskList();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    )::get;

    if (durableStageStorageEnabled) {
      return DurableStorageInputChannelFactory.createStandardImplementation(
          task.getControllerTaskId(),
          workerTaskList,
          TalariaTasks.makeStorageConnector(context.injector()),
          closer
      );
    } else {
      return new WorkerOrLocalInputChannelFactory(workerTaskList);
    }
  }

  private OutputChannelFactory makeStageOutputChannelFactory(final FrameContext frameContext, final int stageNumber)
  {
    // Use the standard frame size, since we assume this size when computing how much is needed to merge output
    // files from different workers.
    final int frameSize = frameContext.memoryParameters().getStandardFrameSize();

    if (durableStageStorageEnabled) {
      return DurableStorageOutputChannelFactory.createStandardImplementation(
          task.getControllerTaskId(),
          id(),
          stageNumber,
          frameSize,
          TalariaTasks.makeStorageConnector(context.injector())
      );
    } else {
      final File fileChannelDirectory =
          new File(context.tempDir(), StringUtils.format("output_stage_%06d", stageNumber));

      return new FileOutputChannelFactory(fileChannelDirectory, frameSize);
    }
  }

  private ListeningExecutorService makeProcessingPool()
  {
    final QueryProcessingPool queryProcessingPool = context.injector().getInstance(QueryProcessingPool.class);
    final int priority = 0;

    return new DecoratedExecutorService(
        queryProcessingPool,
        new DecoratedExecutorService.Decorator()
        {
          @Override
          public <T> Callable<T> decorateCallable(Callable<T> callable)
          {
            return new PrioritizedCallable<T>()
            {
              @Override
              public int getPriority()
              {
                return priority;
              }

              @Override
              public T call() throws Exception
              {
                return callable.call();
              }
            };
          }

          @Override
          public Runnable decorateRunnable(Runnable runnable)
          {
            return new PrioritizedRunnable()
            {
              @Override
              public int getPriority()
              {
                return priority;
              }

              @Override
              public void run()
              {
                runnable.run();
              }
            };
          }
        }
    );
  }

  /**
   * Posts all counters for this worker to the controller.
   */
  private void postCountersToController() throws IOException
  {
    final CounterSnapshotsTree snapshotsTree = getCounters();

    if (leaderAlive && !snapshotsTree.isEmpty()) {
      leaderClient.postCounters(snapshotsTree);
    }
  }

  /**
   * Cleans up the stage outputs corresponding to the provided stage id. It essentially calls {@code doneReading()} on
   * the readable channels corresponding to all the partitions for that stage, and removes it from the {@code stageOutputs}
   * map
   */
  private void cleanStageOutput(final StageId stageId)
  {
    // This code is thread-safe because remove() on ConcurrentHashMap will remove and return the removed channel only for
    // one thread. For the other threads it will return null, therefore we will call doneReading for a channel only once
    final ConcurrentHashMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.remove(stageId);
    // Check for null, this can be the case if this method is called simultaneously from multiple threads.
    if (partitionOutputsForStage == null) {
      return;
    }
    for (final int partition : partitionOutputsForStage.keySet()) {
      final ReadableFrameChannel output = partitionOutputsForStage.remove(partition);
      if (output == null) {
        continue;
      }
      output.doneReading();

      // One caveat with this approach is that in case of a worker crash, while the MM/Indexer systems will delete their
      // temp directories where intermediate results were stored, it won't be the case for the external storage.
      // Therefore, the logic for cleaning the stage output in case of a worker/machine crash has to be external. We currently take care of this in the leader.
      if (durableStageStorageEnabled) {
        final String fileName = DurableStorageOutputChannelFactory.getPartitionFileName(
            task.getControllerTaskId(),
            task.getId(),
            stageId.getStageNumber(),
            partition
        );
        try {
          TalariaTasks.makeStorageConnector(context.injector()).deleteFile(fileName);
        }
        catch (Exception e) {
          // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
          log.warn(e, "Error while cleaning up temporary files at path " + fileName);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  private SettableFuture<ClusterByPartitions> startWorkOrder(
      final WorkerStageKernel kernel,
      final InputChannelFactory inputChannelFactory,
      final CounterTracker counters,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final int parallelism,
      final FrameContext frameContext,
      final TalariaWarningReportPublisher talariaWarningReportPublisher
  ) throws IOException
  {
    final WorkOrder workOrder = kernel.getWorkOrder();
    final int workerNumber = workOrder.getWorkerNumber();
    final StageDefinition stageDef = workOrder.getStageDefinition();

    final InputChannels inputChannels =
        InputChannels.create(
            workOrder.getQueryDefinition(),
            InputSlices.allReadablePartitions(workOrder.getInputs()),
            inputChannelFactory,
            () -> ArenaMemoryAllocator.createOnHeap(frameContext.memoryParameters().getStandardFrameSize()),
            exec,
            cancellationId
        );

    final InputSliceReader inputSliceReader = makeInputSliceReader(
        workOrder.getQueryDefinition(),
        inputChannels,
        frameContext.tempDir(),
        frameContext.dataSegmentProvider()
    );

    final OutputChannelFactory workerOutputChannelFactory;

    if (stageDef.doesShuffle()) {
      // Writing to a consumer in the same JVM (which will be set up later on in this method). Use the large frame
      // size, since we may be writing to a SuperSorter, and we'll generate fewer temp files if we use larger frames.
      // Note: it's not *guaranteed* that we're writing to a SuperSorter, but it's harmless to use large frames
      // even if not.
      workerOutputChannelFactory =
          new BlockingQueueOutputChannelFactory(frameContext.memoryParameters().getLargeFrameSize());
    } else {
      // Writing stage output.
      workerOutputChannelFactory = makeStageOutputChannelFactory(frameContext, stageDef.getStageNumber());
    }

    final ResultAndChannels<?> workerResultAndOutputChannels =
        makeAndRunWorkers(
            workerNumber,
            workOrder.getStageDefinition().getProcessorFactory(),
            workOrder.getExtraInfo(),
            new CountingOutputChannelFactory(
                workerOutputChannelFactory,
                counters.channel(CounterNames.outputChannel())
            ),
            stageDef,
            workOrder.getInputs(),
            inputSliceReader,
            frameContext,
            exec,
            cancellationId,
            parallelism,
            processorBouncer,
            counters,
            talariaWarningReportPublisher
        );

    final ListenableFuture<ClusterByPartitions> stagePartitionBoundariesFuture;
    final ListenableFuture<OutputChannels> outputChannelsFuture;

    if (stageDef.doesShuffle()) {
      final ClusterBy clusterBy = workOrder.getStageDefinition().getShuffleSpec().get().getClusterBy();

      final CountingOutputChannelFactory shuffleOutputChannelFactory =
          new CountingOutputChannelFactory(
              makeStageOutputChannelFactory(frameContext, stageDef.getStageNumber()),
              counters.channel(CounterNames.sortChannel())
          );

      if (stageDef.doesSortDuringShuffle()) {
        if (stageDef.mustGatherResultKeyStatistics()) {
          stagePartitionBoundariesFuture = SettableFuture.create();
        } else {
          stagePartitionBoundariesFuture = Futures.immediateFuture(kernel.getResultPartitionBoundaries());
        }

        outputChannelsFuture = superSortOutputChannels(
            workOrder.getStageDefinition(),
            clusterBy,
            workerResultAndOutputChannels.getOutputChannels(),
            stagePartitionBoundariesFuture,
            shuffleOutputChannelFactory,
            exec,
            cancellationId,
            frameContext.memoryParameters(),
            context,
            kernelManipulationQueue,
            counters.sortProgress()
        );
      } else {
        // No sorting, just combining all outputs into one big partition. Use a muxer to get everything into one file.
        // Note: even if there is only one output channel, we'll run it through the muxer anyway, to ensure the data
        // gets written to a file. (httpGetChannelData requires files.)
        final OutputChannel outputChannel = shuffleOutputChannelFactory.openChannel(0);

        final FrameChannelMuxer muxer =
            new FrameChannelMuxer(
                workerResultAndOutputChannels.getOutputChannels()
                                             .getAllChannels()
                                             .stream()
                                             .map(OutputChannel::getReadableChannel)
                                             .collect(Collectors.toList()),
                outputChannel.getWritableChannel()
            );

        //noinspection unchecked, rawtypes
        outputChannelsFuture = Futures.transform(
            exec.runFully(muxer, cancellationId),
            (Function) ignored -> OutputChannels.wrap(Collections.singletonList(outputChannel.readOnly()))
        );

        stagePartitionBoundariesFuture = null;
      }
    } else {
      stagePartitionBoundariesFuture = null;

      // Retain read-only versions to reduce memory footprint.
      outputChannelsFuture = Futures.immediateFuture(workerResultAndOutputChannels.getOutputChannels().readOnly());
    }

    // Output channels and future are all constructed. Sanity check, record them, and set up callbacks.
    Futures.addCallback(
        Futures.allAsList(
            Arrays.asList(
                workerResultAndOutputChannels.getResultFuture(),
                Futures.transform(
                    outputChannelsFuture,
                    new Function<OutputChannels, OutputChannels>()
                    {
                      @Override
                      public OutputChannels apply(final OutputChannels channels)
                      {
                        sanityCheckOutputChannels(channels);
                        return channels;
                      }
                    }
                )
            )
        ),
        new FutureCallback<List<Object>>()
        {
          @Override
          public void onSuccess(final List<Object> workerResultAndOutputChannelsResolved)
          {
            Object resultObject = workerResultAndOutputChannelsResolved.get(0);
            final OutputChannels outputChannels = (OutputChannels) workerResultAndOutputChannelsResolved.get(1);

            for (OutputChannel channel : outputChannels.getAllChannels()) {
              stageOutputs.computeIfAbsent(stageDef.getId(), ignored1 -> new ConcurrentHashMap<>())
                          .computeIfAbsent(channel.getPartitionNumber(), ignored2 -> channel.getReadableChannel());
            }
            kernelManipulationQueue.add(holder -> holder.getStageKernelMap()
                                                        .get(stageDef.getId())
                                                        .setResultsComplete(resultObject));
          }

          @Override
          public void onFailure(final Throwable t)
          {
            kernelManipulationQueue.add(
                kernelHolder ->
                    kernelHolder.getStageKernelMap().get(stageDef.getId()).fail(t)
            );
          }
        }
    );

    // Return settable result-key-statistics future, so callers can set it and unblock the supersorter if needed.
    return stageDef.mustGatherResultKeyStatistics()
           ? (SettableFuture<ClusterByPartitions>) stagePartitionBoundariesFuture
           : null;
  }

  private static <FactoryType extends FrameProcessorFactory<I, WorkerClass, T, R>, I, WorkerClass extends FrameProcessor<T>, T, R> ResultAndChannels<R> makeAndRunWorkers(
      final int workerNumber,
      final FactoryType processorFactory,
      final I processorFactoryExtraInfo,
      final OutputChannelFactory outputChannelFactory,
      final StageDefinition stageDefinition,
      final List<InputSlice> inputSlices,
      final InputSliceReader inputSliceReader,
      final FrameContext frameContext,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final int parallelism,
      final Bouncer processorBouncer,
      final CounterTracker counters,
      final TalariaWarningReportPublisher warningPublisher
  ) throws IOException
  {
    final ProcessorsAndChannels<WorkerClass, T> processors =
        processorFactory.makeProcessors(
            stageDefinition,
            workerNumber,
            inputSlices,
            inputSliceReader,
            processorFactoryExtraInfo,
            outputChannelFactory,
            frameContext,
            parallelism,
            counters,
            e -> warningPublisher.publishException(stageDefinition.getStageNumber(), e)
        );

    final Sequence<WorkerClass> processorSequence = processors.processors();

    final int maxOutstandingProcessors;

    if (processors.getOutputChannels().getAllChannels().isEmpty()) {
      // No output channels: run up to "parallelism" processors at once.
      maxOutstandingProcessors = Math.max(1, parallelism);
    } else {
      // If there are output channels, that acts as a ceiling on the number of processors that can run at once.
      maxOutstandingProcessors =
          Math.max(1, Math.min(parallelism, processors.getOutputChannels().getAllChannels().size()));
    }

    final ListenableFuture<R> workResultFuture = FrameProcessors.runAllFully(
        processorSequence,
        exec,
        processorFactory.newAccumulatedResult(),
        processorFactory::accumulateResult,
        maxOutstandingProcessors,
        processorBouncer,
        cancellationId
    );

    return new ResultAndChannels<>(workResultFuture, processors.getOutputChannels());
  }

  private static InputSliceReader makeInputSliceReader(
      final QueryDefinition queryDef,
      final InputChannels inputChannels,
      final File temporaryDirectory,
      final DataSegmentProvider segmentProvider
  )
  {
    return new MapInputSliceReader(
        ImmutableMap.<Class<? extends InputSlice>, InputSliceReader>builder()
                    .put(NilInputSlice.class, NilInputSliceReader.INSTANCE)
                    .put(StageInputSlice.class, new StageInputSliceReader(queryDef.getQueryId(), inputChannels))
                    .put(ExternalInputSlice.class, new ExternalInputSliceReader(temporaryDirectory))
                    .put(SegmentsInputSlice.class, new SegmentsInputSliceReader(segmentProvider))
                    .build()
    );
  }

  private static ListenableFuture<OutputChannels> superSortOutputChannels(
      final StageDefinition stageDefinition,
      final ClusterBy clusterBy,
      final OutputChannels processorOutputChannels,
      final ListenableFuture<ClusterByPartitions> stagePartitionBoundariesFuture,
      final OutputChannelFactory outputChannelFactory,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final WorkerMemoryParameters memoryParameters,
      final WorkerContext context,
      final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue,
      final SuperSorterProgressTracker superSorterProgressTracker
  ) throws IOException
  {
    if (!stageDefinition.doesShuffle()) {
      throw new ISE("Output channels do not need shuffling");
    }

    final List<ReadableFrameChannel> channelsToSuperSort;

    if (processorOutputChannels.getAllChannels().isEmpty()) {
      // No data coming out of this processor. Report empty statistics, if the kernel is expecting statistics.
      if (stageDefinition.mustGatherResultKeyStatistics()) {
        kernelManipulationQueue.add(
            holder ->
                holder.getStageKernelMap().get(stageDefinition.getId())
                      .setResultKeyStatisticsSnapshot(ClusterByStatisticsSnapshot.empty())
        );
      }

      // Process one empty channel so the SuperSorter has something to do.
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      channel.doneWriting();
      channelsToSuperSort = Collections.singletonList(channel);
    } else if (stageDefinition.mustGatherResultKeyStatistics()) {
      channelsToSuperSort = collectKeyStatistics(
          stageDefinition,
          clusterBy,
          processorOutputChannels,
          exec,
          cancellationId,
          kernelManipulationQueue
      );
    } else {
      channelsToSuperSort = processorOutputChannels.getAllChannels()
                                                   .stream()
                                                   .map(OutputChannel::getReadableChannel)
                                                   .collect(Collectors.toList());
    }

    final File sorterTmpDir = new File(context.tempDir(), "super-sort-" + UUID.randomUUID());
    FileUtils.mkdirp(sorterTmpDir);
    if (!sorterTmpDir.isDirectory()) {
      throw new IOException("Cannot create directory: " + sorterTmpDir);
    }

    final SuperSorter sorter = new SuperSorter(
        channelsToSuperSort,
        stageDefinition.getFrameReader(),
        clusterBy,
        stagePartitionBoundariesFuture,
        exec,
        sorterTmpDir,
        outputChannelFactory,
        () -> ArenaMemoryAllocator.createOnHeap(memoryParameters.getLargeFrameSize()),
        memoryParameters.getSuperSorterMaxActiveProcessors(),
        memoryParameters.getSuperSorterMaxChannelsPerProcessor(),
        -1,
        cancellationId,
        superSorterProgressTracker
    );

    return sorter.run();
  }

  private static List<ReadableFrameChannel> collectKeyStatistics(
      final StageDefinition stageDefinition,
      final ClusterBy clusterBy,
      final OutputChannels processorOutputChannels,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue
  )
  {
    final List<ReadableFrameChannel> retVal = new ArrayList<>();
    final List<KeyStatisticsCollectionProcessor> processors = new ArrayList<>();

    for (final OutputChannel outputChannel : processorOutputChannels.getAllChannels()) {
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      retVal.add(channel);

      processors.add(
          new KeyStatisticsCollectionProcessor(
              outputChannel.getReadableChannel(),
              channel,
              stageDefinition.getFrameReader(),
              clusterBy,
              stageDefinition.createResultKeyStatisticsCollector()
          )
      );
    }

    final ListenableFuture<ClusterByStatisticsCollector> clusterByStatisticsCollectorFuture =
        FrameProcessors.runAllFully(
            Sequences.simple(processors),
            exec,
            stageDefinition.createResultKeyStatisticsCollector(),
            ClusterByStatisticsCollector::addAll,
            // Run all processors simultaneously. They are lightweight and this keeps things moving.
            processors.size(),
            Bouncer.unlimited(),
            cancellationId
        );

    Futures.addCallback(
        clusterByStatisticsCollectorFuture,
        new FutureCallback<ClusterByStatisticsCollector>()
        {
          @Override
          public void onSuccess(final ClusterByStatisticsCollector result)
          {
            kernelManipulationQueue.add(
                holder ->
                    holder.getStageKernelMap().get(stageDefinition.getId())
                          .setResultKeyStatisticsSnapshot(result.snapshot())
            );
          }

          @Override
          public void onFailure(Throwable t)
          {
            kernelManipulationQueue.add(
                holder -> {
                  log.noStackTrace()
                     .warn(t, "Failed to gather clusterBy statistics for stage [%s]", stageDefinition.getId());
                  holder.getStageKernelMap().get(stageDefinition.getId()).fail(t);
                }
            );
          }
        }
    );

    return retVal;
  }

  private static void sanityCheckOutputChannels(final OutputChannels outputChannels)
  {
    // Verify there is exactly one channel per partition.
    for (int partitionNumber : outputChannels.getPartitionNumbers()) {
      final List<OutputChannel> outputChannelsForPartition =
          outputChannels.getChannelsForPartition(partitionNumber);

      Preconditions.checkState(partitionNumber >= 0, "Expected partitionNumber >= 0, but got [%s]", partitionNumber);
      Preconditions.checkState(
          outputChannelsForPartition.size() == 1,
          "Expected one channel for partition [%s], but got [%s]",
          partitionNumber,
          outputChannelsForPartition.size()
      );
    }
  }

  /**
   * Log (at DEBUG level) a string explaining the status of all work assigned to this worker.
   */
  private static void logKernelStatus(final Collection<WorkerStageKernel> kernels)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Stages: %s",
          kernels.stream()
                 .sorted(Comparator.comparing(k -> k.getStageDefinition().getStageNumber()))
                 .map(WorkerImpl::makeKernelStageStatusString)
                 .collect(Collectors.joining("; "))
      );
    }
  }

  /**
   * Helper used by {@link #logKernelStatus}.
   */
  private static String makeKernelStageStatusString(final WorkerStageKernel kernel)
  {
    final String inputPartitionNumbers =
        StreamSupport.stream(InputSlices.allReadablePartitions(kernel.getWorkOrder().getInputs()).spliterator(), false)
                     .map(ReadablePartition::getPartitionNumber)
                     .sorted()
                     .map(String::valueOf)
                     .collect(Collectors.joining(","));

    // String like ">50" if shuffling to 50 partitions, ">?" if shuffling to unknown number of partitions.
    final String shuffleStatus =
        kernel.getStageDefinition().doesShuffle()
        ? ">" + (kernel.hasResultPartitionBoundaries() ? kernel.getResultPartitionBoundaries().size() : "?")
        : "";

    return StringUtils.format(
        "S%d:W%d:P[%s]%s:%s:%s",
        kernel.getStageDefinition().getStageNumber(),
        kernel.getWorkOrder().getWorkerNumber(),
        inputPartitionNumbers,
        shuffleStatus,
        kernel.getStageDefinition().doesShuffle() ? "SHUFFLE" : "RETAIN",
        kernel.getPhase()
    );
  }

  /**
   * An {@link InputChannelFactory} that loads data locally when possible, and otherwise connects directly to other
   * workers. Used when durable shuffle storage is off.
   */
  private class WorkerOrLocalInputChannelFactory implements InputChannelFactory
  {
    private final Supplier<List<String>> taskList;
    private final WorkerInputChannelFactory workerInputChannelFactory;

    public WorkerOrLocalInputChannelFactory(final Supplier<List<String>> taskList)
    {
      this.workerInputChannelFactory = new WorkerInputChannelFactory(workerClient, taskList);
      this.taskList = taskList;
    }

    @Override
    public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber)
    {
      final String taskId = taskList.get().get(workerNumber);
      if (taskId.equals(id())) {
        final ConcurrentMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.get(stageId);
        if (partitionOutputsForStage == null) {
          throw new ISE("Unable to find outputs for stage: [%s]", stageId);
        }

        final ReadableFrameChannel myChannel = partitionOutputsForStage.get(partitionNumber);

        if (myChannel instanceof ReadableFileFrameChannel) {
          // Must duplicate the channel to avoid double-closure upon task cleanup.
          final FrameFile frameFile = ((ReadableFileFrameChannel) myChannel).newFrameFileReference();
          return new ReadableFileFrameChannel(frameFile);
        } else if (myChannel instanceof ReadableNilFrameChannel) {
          return myChannel;
        } else {
          throw new ISE("Output for stage: [%s] are stored in an instance of %s which is not "
                        + "supported", stageId, myChannel.getClass());
        }
      } else {
        return workerInputChannelFactory.openChannel(stageId, workerNumber, partitionNumber);
      }
    }
  }

  private static class KernelHolder
  {
    private final Map<StageId, WorkerStageKernel> stageKernelMap = new HashMap<>();
    private boolean done = false;

    public Map<StageId, WorkerStageKernel> getStageKernelMap()
    {
      return stageKernelMap;
    }

    public boolean isDone()
    {
      return done;
    }

    public void setDone()
    {
      this.done = true;
    }
  }

  private static class ResultAndChannels<T>
  {
    private final ListenableFuture<T> resultFuture;
    private final OutputChannels outputChannels;

    public ResultAndChannels(
        ListenableFuture<T> resultFuture,
        OutputChannels outputChannels
    )
    {
      this.resultFuture = resultFuture;
      this.outputChannels = outputChannels;
    }

    public ListenableFuture<T> getResultFuture()
    {
      return resultFuture;
    }

    public OutputChannels getOutputChannels()
    {
      return outputChannels;
    }
  }
}
