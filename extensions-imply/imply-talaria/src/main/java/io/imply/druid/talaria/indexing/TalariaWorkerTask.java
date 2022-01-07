/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.frame.MemoryAllocator;
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
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessorExecutor;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.OutputChannel;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.frame.processor.SuperSorter;
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.kernel.worker.WorkerStageKernel;
import io.imply.druid.talaria.kernel.worker.WorkerStagePhase;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import io.imply.druid.talaria.querykit.LazyResourceHolder;
import io.imply.druid.talaria.util.DecoratedExecutorService;
import io.imply.druid.talaria.util.FutureUtils;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.PrioritizedCallable;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@JsonTypeName(TalariaWorkerTask.TYPE)
public class TalariaWorkerTask extends AbstractTask implements ChatHandler
{
  public static final String TYPE = "talaria1";
  private static final Logger log = new Logger(TalariaWorkerTask.class);

  // TODO(gianm): handle parallelism way less jankily
  private static final int MAX_SUPER_SORTER_PROCESSORS = 4;
  private static final int MINIMUM_SUPER_SORTER_FRAMES = 3; // 2 input frames, 1 output frame

  private final String controllerTaskId;
  private final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue = new LinkedBlockingDeque<>();
  private final ConcurrentMap<String, QueryDefinition> queryDefinitionMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<StagePartition, OutputChannel> outputChannelMap = new ConcurrentHashMap<>();
  private final TalariaCounters talariaCounters = new TalariaCounters();

  private volatile TaskToolbox toolbox;
  private volatile DruidNode selfDruidNode;
  private volatile TalariaTaskClient netClient;
  private volatile Bouncer processorBouncer;
  private volatile SegmentCacheManager segmentCacheManager;
  private volatile IndexIO indexIO;

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector = null;

  @JsonCreator
  TalariaWorkerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("controllerTaskId") final String controllerTaskId,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSource),
        groupId,
        null,
        dataSource,
        context
    );

    this.controllerTaskId = controllerTaskId;
  }

  @JsonProperty
  private String getControllerTaskId()
  {
    return controllerTaskId;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(final TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    try (final Closer closer = Closer.create()) {
      Throwable exceptionEncountered = null;
      Optional<TalariaErrorReport> runTaskResult;

      try {
        runTaskResult = runTask(toolbox, closer);
      }
      catch (Throwable e) {
        exceptionEncountered = e;
        runTaskResult = Optional.of(
            TalariaErrorReport.fromException(getId(), TalariaTasks.getHostFromSelfNode(selfDruidNode), null, e)
        );
      }

      if (runTaskResult.isPresent()) {
        final TalariaErrorReport errorReport = runTaskResult.get();

        final StringBuilder logMessage = new StringBuilder("Work failed");
        if (errorReport.getStageNumber() != null) {
          logMessage.append(" (stage ").append(errorReport.getStageNumber()).append(")");
        }
        logMessage.append(": ").append(errorReport.getFault().getCodeWithMessage());

        if (exceptionEncountered == null) {
          log.warn(logMessage.toString());
        } else {
          log.noStackTrace().warn(exceptionEncountered, logMessage.toString());
        }

        closer.register(() -> {
          if (netClient != null && selfDruidNode != null) {
            netClient.postWorkerError(
                getControllerTaskId(),
                getId(),
                errorReport
            );
          }
        });

        return TaskStatus.failure(getId(), errorReport.getFault().getCodeWithMessage());
      } else {
        return TaskStatus.success(getId());
      }
    }
  }

  public Optional<TalariaErrorReport> runTask(final TaskToolbox toolbox, final Closer closer) throws Exception
  {
    this.toolbox = toolbox;
    this.selfDruidNode = injector.getInstance(Key.get(DruidNode.class, Self.class));
    this.netClient = makeTaskClient();
    closer.register(netClient::close);
    this.processorBouncer = injector.getInstance(Bouncer.class);
    this.segmentCacheManager = injector.getInstance(SegmentCacheManagerFactory.class)
                                       .manufacturate(new File(toolbox.getIndexingTmpDir(), "segment-fetch"));
    this.indexIO = injector.getInstance(IndexIO.class);

    toolbox.getChatHandlerProvider().register(getId(), this, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(getId()));

    final KernelHolder kernelHolder = new KernelHolder();
    final String cancellationId = getId();

    final Supplier<MemoryAllocator> allocatorMaker = () ->
        ArenaMemoryAllocator.create(ByteBuffer.allocate(MemoryLimits.FRAME_SIZE));

    final FrameProcessorFactory.ProviderThingy providerThingy = makeProviderThingy(toolbox, allocatorMaker);

    // TODO(gianm): we need visibility into the thread pool size so we know how many workers should be created at once
    final int numThreads = providerThingy.provide(DruidProcessingConfig.class).getNumThreads();

    final FrameProcessorExecutor workerExec = new FrameProcessorExecutor(makeProcessingPool());
    closer.register(() -> workerExec.cancel(cancellationId));

    // TODO(gianm): consider using a different thread pool for connecting
    final InputChannelFactory inputChannelFactory = makeBaseInputChannelFactory(workerExec.getExecutorService());
    final OutputChannelFactory baseOutputChannelFactory = makeBaseOutputChannelFactory();

    final Map<StageId, SettableFuture<ClusterByPartitions>> partitionBoundariesFutureMap = new HashMap<>();

    // TODO(gianm): push this into kernel
    final Set<Pair<StageId, Integer>> postedResultsComplete = new HashSet<>();

    // TODO(gianm): hack alert!! need to know max # of workers in jvm for memory allocations
    final int workersInJvm;
    if (toolbox.getAppenderatorsManager() instanceof UnifiedIndexerAppenderatorsManager) {
      // CliIndexer
      workersInJvm = injector.getInstance(WorkerConfig.class).getCapacity();
    } else {
      // CliPeon
      workersInJvm = 1;
    }

    SuperSorterParameters superSorterParameters = null;

    while (!kernelHolder.isDone()) {
      boolean didSomething = false;

      for (final WorkerStageKernel kernel : kernelHolder.getStageKernelMap().values()) {
        final StageDefinition stageDefinition = kernel.getStageDefinition();

        if (kernel.getPhase() == WorkerStagePhase.NEW) {
          log.debug("New work order: %s", toolbox.getJsonMapper().writeValueAsString(kernel.getWorkOrder()));

          // Verify memory *now*, instead of before receiving the work order, to ensure that the error can
          // propagate back up to the controller.
          if (superSorterParameters == null) {
            superSorterParameters = SuperSorterParameters.compute(
                Runtime.getRuntime().maxMemory(),
                workersInJvm,
                processorBouncer.getMaxCount(),
                MAX_SUPER_SORTER_PROCESSORS
            );
          }

          // Start working on this stage immediately.
          // TODO(gianm): Resource control -- may not be able to start up these channels immediately
          kernel.startReading();
          final SettableFuture<ClusterByPartitions> partitionBoundariesFuture =
              startWorkOrder(
                  kernel,
                  inputChannelFactory,
                  baseOutputChannelFactory,
                  talariaCounters,
                  workerExec,
                  cancellationId,
                  numThreads,
                  allocatorMaker,
                  superSorterParameters,
                  providerThingy
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
          // TODO(gianm): Do this in a different thread?
          netClient.postKeyStatistics(
              controllerTaskId,
              stageDefinition.getId(),
              kernel.getWorkOrder().getWorkerNumber(),
              kernel.getResultKeyStatisticsSnapshot()
          );

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
            && postedResultsComplete.add(Pair.of(stageDefinition.getId(), kernel.getWorkOrder().getWorkerNumber()))) {
          // TODO(gianm): Do this in a different thread?
          netClient.postResultsComplete(
              controllerTaskId,
              stageDefinition.getId(),
              kernel.getWorkOrder().getWorkerNumber(),
              kernel.getResultObject()
          );
        }

        if (kernel.getPhase() == WorkerStagePhase.FAILED) {
          // TODO(gianm): Enable retries, somehow?
          return Optional.of(
              TalariaErrorReport.fromException(
                  getId(),
                  TalariaTasks.getHostFromSelfNode(selfDruidNode),
                  stageDefinition.getId().getStageNumber(),
                  kernel.getException()
              )
          );
        }
      }

      if (!didSomething && !kernelHolder.isDone()) {
        // TODO(gianm): find a better way to report counters, turn this back to kmq.take()
        Consumer<KernelHolder> nextCommand;
        String countersString = null;

        do {
          if (log.isDebugEnabled()) {
            final String nextCountersString = talariaCounters.stateString();
            if (!nextCountersString.equals(countersString)) {
              log.debug("Counters: %s", nextCountersString);
              countersString = nextCountersString;
            }
          }

          if (!queryDefinitionMap.isEmpty()) {
            netClient.postCounters(controllerTaskId, getId(), talariaCounters.snapshot());
          }
        } while ((nextCommand = kernelManipulationQueue.poll(5, TimeUnit.SECONDS)) == null);

        nextCommand.accept(kernelHolder);
        logKernelStatus(kernelHolder.getStageKernelMap().values());
      }
    }

    // Empty means success.
    return Optional.empty();
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    // TODO(gianm): Do something else, since this doesn't seem right
    kernelManipulationQueue.add(KernelHolder::setDone);
  }

  /**
   * See {@link TalariaTaskClient#getChannelData} for the client-side code that calls this API.
   */
  @GET
  @Path("/channels/{queryId}/{stageNumber}/{partitionNumber}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response httpGetChannelData(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("partitionNumber") final int partitionNumber,
      @QueryParam("offset") final long offset,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    final StagePartition stagePartition = new StagePartition(new StageId(queryId, stageNumber), partitionNumber);
    final OutputChannel channel = outputChannelMap.get(stagePartition);

    if (channel == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    final ReadableFrameChannel actualChannel;
    try {
      actualChannel = channel.getReadableChannel();
    }
    catch (Exception e) {
      // TODO(gianm): not sure if this response makes sense.
      log.noStackTrace()
         .warn(e, "Returned server error to client because channel for [%s] could not be acquired", stagePartition);

      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    if (actualChannel instanceof ReadableNilFrameChannel) {
      // TODO(gianm): support "offset" for nil channels
      return Response.ok(
          (StreamingOutput) outputStream -> {
            // Build an empty frame file.
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            FrameFileWriter.open(Channels.newChannel(baos)).close();

            final ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());

            //noinspection ResultOfMethodCallIgnored: OK to ignore since "skip" always works for ByteArrayInputStream.
            in.skip(offset);

            //noinspection UnstableApiUsage
            ByteStreams.copy(in, outputStream);
          }
      ).build();
    } else if (actualChannel instanceof ReadableFileFrameChannel) {
      return Response.ok(
          (StreamingOutput) outputStream -> {
            try (final FrameFile frameFile = ((ReadableFileFrameChannel) actualChannel).getFrameFileReference();
                 final RandomAccessFile randomAccessFile = new RandomAccessFile(frameFile.file(), "r")) {
              randomAccessFile.seek(offset);

              //noinspection UnstableApiUsage
              ByteStreams.copy(Channels.newInputStream(randomAccessFile.getChannel()), outputStream);
            }
          }
      ).build();
    } else {
      log.warn(
          "Returned server error to client because channel for [%s] is not nil or file-based (class = %s)",
          stagePartition,
          actualChannel.getClass().getName()
      );

      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * See {@link TalariaTaskClient#postWorkOrder} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workOrder")
  public Response httpPostWorkOrder(final WorkOrder workOrder, @Context final HttpServletRequest req)
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    // TODO(gianm): Prevent conflicts but retain idempotency (must save WorkOrder?)
    queryDefinitionMap.putIfAbsent(workOrder.getQueryDefinition().getQueryId(), workOrder.getQueryDefinition());

    kernelManipulationQueue.add(
        kernelHolder ->
            kernelHolder.getStageKernelMap().computeIfAbsent(
                workOrder.getStageDefinition().getId(),
                ignored -> WorkerStageKernel.create(workOrder)
            )
    );

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link TalariaTaskClient#postResultPartitionBoundaries} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/resultPartitionBoundaries/{queryId}/{stageNumber}")
  public Response httpPostResultPartitionBoundaries(
      final Object stagePartitionBoundariesObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    final StageId stageId = new StageId(queryId, stageNumber);
    final QueryDefinition queryDef = queryDefinitionMap.get(queryId);

    if (queryDef == null) {
      // TODO(gianm): improve error?
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    // We need a specially-decorated ObjectMapper to deserialize partition boundaries.
    final StageDefinition stageDef = queryDef.getStageDefinition(stageNumber);
    final ObjectMapper decoratedObjectMapper =
        TalariaTasks.decorateObjectMapperForClusterByKey(
            toolbox.getJsonMapper(),
            stageDef.getSignature(),
            queryDef.getClusterByForStage(stageNumber),
            stageDef.getShuffleSpec().map(ShuffleSpec::doesAggregateByClusterKey).orElse(false)
        );

    final ClusterByPartitions stagePartitionBoundaries =
        decoratedObjectMapper.convertValue(stagePartitionBoundariesObject, ClusterByPartitions.class);

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

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link TalariaTaskClient#postFinish} for the client-side code that calls this API.
   */
  @POST
  @Path("/finish")
  public Response httpPostFinish(@Context final HttpServletRequest req)
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    kernelManipulationQueue.add(KernelHolder::setDone);

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link TalariaTaskClient#getCounters} for the client-side code that calls this API.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/counters")
  public Response httpGetCounters(@Context final HttpServletRequest req)
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    return Response.status(Response.Status.OK).entity(talariaCounters.snapshot()).build();
  }

  private InputChannelFactory makeBaseInputChannelFactory(final ExecutorService connectExec)
  {
    return new InputChannelFactory()
    {
      // TODO(gianm): Handle failures, retries of other tasks (changing task list)
      final Supplier<List<String>> taskList = Suppliers.memoize(
          () -> netClient.getTaskList(controllerTaskId).orElseThrow(
              () -> new ISE("Really expected tasks to be available by now")
          )
      )::get;

      @Override
      public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber)
      {
        final String taskId = taskList.get().get(workerNumber);
        if (taskId.equals(getId())) {
          final ReadableFrameChannel myChannel =
              outputChannelMap.get(new StagePartition(stageId, partitionNumber)).getReadableChannel();

          if (myChannel instanceof ReadableFileFrameChannel) {
            // Must duplicate the channel to avoid double-closure upon task cleanup.
            final FrameFile frameFile = ((ReadableFileFrameChannel) myChannel).getFrameFileReference();
            return new ReadableFileFrameChannel(frameFile);
          } else if (myChannel instanceof ReadableNilFrameChannel) {
            return myChannel;
          } else {
            // TODO(gianm): eek
            throw new UnsupportedOperationException();
          }
        } else {
          return netClient.getChannelData(taskId, stageId, partitionNumber, connectExec);
        }
      }
    };
  }

  private OutputChannelFactory makeBaseOutputChannelFactory()
  {
    final File fileChannelDirectory = new File(toolbox.getIndexingTmpDir(), "file-channels");
    return new FileOutputChannelFactory(fileChannelDirectory);
  }

  private ListeningExecutorService makeProcessingPool()
  {
    final QueryProcessingPool queryProcessingPool = injector.getInstance(QueryProcessingPool.class);
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

  private FrameProcessorFactory.ProviderThingy makeProviderThingy(
      final TaskToolbox toolbox,
      final Supplier<MemoryAllocator> allocatorMaker
  )
  {
    return new FrameProcessorFactory.ProviderThingy()
    {
      @Override
      @SuppressWarnings("unchecked")
      public <T> T provide(Class<T> clazz)
      {
        if (clazz == TaskToolbox.class) {
          return (T) toolbox;
        } else if (clazz == RowIngestionMeters.class) {
          // TODO(gianm): These are just thrown away!
          return (T) (toolbox.getRowIngestionMetersFactory().createRowIngestionMeters());
        } else if (clazz == MemoryAllocator.class) {
          return (T) allocatorMaker.get();
        } else if (clazz == File.class) {
          // TODO(gianm): this means "temp dir" -- HACK HACK HACK even more than the provider concept in general!
          return (T) toolbox.getIndexingTmpDir();
        } else if (clazz == DataSegmentProvider.class) {
          return (T) new DataSegmentProvider()
          {
            @Override
            public LazyResourceHolder<Segment> fetchSegment(DataSegment dataSegment)
            {
              try {
                return new LazyResourceHolder<>(
                    () -> {
                      try {
                        final Closer closer = Closer.create();
                        final File segmentDir = segmentCacheManager.getSegmentFiles(dataSegment);
                        closer.register(() -> FileUtils.deleteDirectory(segmentDir));

                        final QueryableIndex index = indexIO.loadIndex(segmentDir);
                        closer.register(index);

                        return Pair.of(new QueryableIndexSegment(index, dataSegment.getId()), closer);
                      }
                      catch (IOException | SegmentLoadingException e) {
                        throw new RuntimeException(e);
                      }
                    }
                );
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };
        } else {
          return injector.getInstance(clazz);
        }
      }
    };
  }

  private TalariaTaskClient makeTaskClient()
  {
    return new TalariaTaskClient(
        injector.getInstance(Key.get(HttpClient.class, EscalatedGlobal.class)),
        toolbox.getJsonMapper(),
        new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
        Duration.standardMinutes(360) /* TODO(gianm): hardcoded to large timeout so phaseOneData doesn't fail  */,
        getId(),
        999999 /* TODO(gianm): hardcoded to a very large number so phaseOneData doesn't fail; handle better */
    );
  }

  @Nullable
  private SettableFuture<ClusterByPartitions> startWorkOrder(
      final WorkerStageKernel kernel,
      final InputChannelFactory inputChannelFactory,
      final OutputChannelFactory fileOutputChannelFactory,
      final TalariaCounters counters,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final int parallelism,
      final Supplier<MemoryAllocator> allocatorMaker,
      final SuperSorterParameters superSorterParameters,
      final FrameProcessorFactory.ProviderThingy providerThingy
  ) throws IOException
  {
    final WorkOrder workOrder = kernel.getWorkOrder();
    final int workerNumber = workOrder.getWorkerNumber();
    final StageDefinition stageDef = workOrder.getStageDefinition();
    final ClusterBy clusterBy = workOrder.getQueryDefinition().getClusterByForStage(workOrder.getStageNumber());
    final ReadablePartitions inputPartitions = workOrder.getInputPartitions();

    final InputChannels inputChannels =
        InputChannels.create(
            workOrder.getQueryDefinition(),
            stageDef.getInputStageIds().stream().mapToInt(StageId::getStageNumber).toArray(),
            inputPartitions,
            new CountingInputChannelFactory(
                inputChannelFactory,
                partitionNumber ->
                    counters.getOrCreateChannelCounters(
                        TalariaCounterType.INPUT,
                        workerNumber,
                        stageDef.getStageNumber(),
                        partitionNumber
                    )
            ),
            allocatorMaker,
            exec,
            cancellationId
        );

    // Currently, the *final* final output channel of all stages must be files.
    // For shuffling stages: use an in-memory channel, since it will be shuffled into files later.
    // For non-shuffling stages: write to files directly.
    final OutputChannelFactory workerOutputChannelFactory =
        stageDef.doesShuffle() ? BlockingQueueOutputChannelFactory.INSTANCE : fileOutputChannelFactory;

    // noinspection unchecked
    final ResultAndChannels<?> workerResultAndOutputChannels =
        makeAndRunWorkers(
            workerNumber,
            workOrder.getStageDefinition().getProcessorFactory(),
            workOrder.getExtraInfoHolder().getExtraInfo(),
            inputChannels,
            new CountingOutputChannelFactory(
                workerOutputChannelFactory,
                partitionNumber -> {
                  if (stageDef.doesShuffle()) {
                    return counters.getOrCreateChannelCounters(
                        TalariaCounterType.PRESHUFFLE,
                        workerNumber,
                        stageDef.getStageNumber(),
                        partitionNumber
                    );
                  } else {
                    return counters.getOrCreateChannelCounters(
                        TalariaCounterType.OUTPUT,
                        workerNumber,
                        stageDef.getStageNumber(),
                        partitionNumber
                    );
                  }
                }
            ),
            stageDef.getSignature(),
            workOrder.getQueryDefinition().getClusterByForStage(stageDef.getStageNumber()),
            providerThingy,
            exec,
            cancellationId,
            parallelism,
            processorBouncer
        );

    final ListenableFuture<ClusterByPartitions> stagePartitionBoundariesFuture;
    final ListenableFuture<OutputChannels> outputChannelsFuture;

    if (stageDef.doesShuffle()) {
      final CountingOutputChannelFactory shuffleOutputChannelFactory =
          new CountingOutputChannelFactory(
              fileOutputChannelFactory,
              partitionNumber ->
                  counters.getOrCreateChannelCounters(
                      TalariaCounterType.OUTPUT,
                      workerNumber,
                      stageDef.getStageNumber(),
                      partitionNumber
                  )
          );

      if (clusterBy.getColumns().isEmpty() && kernel.getResultPartitionBoundaries().size() == 1) {
        // Get everything into one big partition.
        // TODO(gianm): Ideally, if there is only one output channel, we should return it directly. But we can't:
        //   it must be run through a muxer so it can get written to a file. (httpGetChannelData requires files.)
        final OutputChannel outputChannel = shuffleOutputChannelFactory.openChannel(0, true);

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
            (Function) ignored -> OutputChannels.wrap(Collections.singletonList(outputChannel))
        );

        stagePartitionBoundariesFuture = null;
      } else {
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
            allocatorMaker,
            superSorterParameters,
            toolbox,
            kernelManipulationQueue
        );
      }
    } else {
      stagePartitionBoundariesFuture = null;
      outputChannelsFuture = Futures.immediateFuture(workerResultAndOutputChannels.getOutputChannels());
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

                        for (OutputChannel channel : channels.getAllChannels()) {
                          outputChannelMap.putIfAbsent(
                              new StagePartition(stageDef.getId(), channel.getPartitionNumber()),
                              channel
                          );
                        }

                        return channels;
                      }
                    }
                )
            )
        ),
        new FutureCallback<List<Object>>()
        {
          @Override
          public void onSuccess(final List<Object> ignored)
          {
            kernelManipulationQueue.add(
                holder ->
                    holder.getStageKernelMap()
                          .get(stageDef.getId())
                          .setResultsComplete(
                              FutureUtils.getUncheckedImmediately(workerResultAndOutputChannels.getResultFuture())
                          )
            );
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
      final InputChannels inputChannels,
      final OutputChannelFactory outputChannelFactory,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final FrameProcessorFactory.ProviderThingy providerThingy,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final int parallelism,
      final Bouncer processorBouncer
  ) throws IOException
  {
    final ProcessorsAndChannels<WorkerClass, T> workers =
        processorFactory.makeProcessors(
            workerNumber,
            processorFactoryExtraInfo,
            inputChannels,
            outputChannelFactory,
            signature,
            clusterBy,
            providerThingy,
            parallelism
        );

    final Sequence<WorkerClass> processorSequence = workers.processors();

    final int maxOutstandingProcessors;

    if (workers.getOutputChannels().getAllChannels().isEmpty()) {
      // No output channels: run up to "parallelism" processors at once.
      maxOutstandingProcessors = Math.max(1, parallelism);
    } else {
      // If there are output channels, that acts as a ceiling on the number of processors that can run at once.
      maxOutstandingProcessors =
          Math.max(1, Math.min(parallelism, workers.getOutputChannels().getAllChannels().size()));
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

    return new ResultAndChannels<>(workResultFuture, workers.getOutputChannels());
  }

  private static ListenableFuture<OutputChannels> superSortOutputChannels(
      final StageDefinition stageDefinition,
      final ClusterBy clusterBy,
      final OutputChannels workerOutputChannels,
      final ListenableFuture<ClusterByPartitions> stagePartitionBoundariesFuture,
      final OutputChannelFactory outputChannelFactory,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final Supplier<MemoryAllocator> allocatorMaker,
      final SuperSorterParameters superSorterParameters,
      final TaskToolbox toolbox,
      final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue
  ) throws IOException
  {
    if (!stageDefinition.doesShuffle()) {
      throw new ISE("Output channels do not need shuffling");
    }

    final List<ReadableFrameChannel> channelsToSuperSort;

    if (workerOutputChannels.getAllChannels().isEmpty()) {
      // No data coming out of this worker.

      // Report empty statistics, if the kernel is expecting statistics.
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
      // TODO(gianm): Remove this silly extra step, which we currently need in order to populate result key stats
      channelsToSuperSort = gatherResultKeyStatistics(
          stageDefinition,
          clusterBy,
          workerOutputChannels,
          exec,
          cancellationId,
          kernelManipulationQueue
      );
    } else {
      channelsToSuperSort = workerOutputChannels.getAllChannels()
                                                .stream()
                                                .map(OutputChannel::getReadableChannel)
                                                .collect(Collectors.toList());
    }

    // TODO(gianm): Check if things are already partitioned properly, and if so, skip the supersorter
    // TODO(gianm): Check if things are already sorted properly, and if so, configure the supersorter appropriately
    final File sorterTmpDir = new File(toolbox.getIndexingTmpDir(), "super-sort-" + UUID.randomUUID());
    if (!sorterTmpDir.mkdir() && !sorterTmpDir.isDirectory()) {
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
        allocatorMaker,
        superSorterParameters.channelsPerProcessor,
        superSorterParameters.channelsPerProcessor * superSorterParameters.numProcessors,
        -1
    );

    return sorter.run();
  }

  private static List<ReadableFrameChannel> gatherResultKeyStatistics(
      final StageDefinition stageDefinition,
      final ClusterBy clusterBy,
      final OutputChannels workerOutputChannels,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue
  )
  {
    final List<ReadableFrameChannel> retVal = new ArrayList<>();
    final List<TalariaClusterByStatisticsCollectorWorker> resultKeyCollectionWorkers = new ArrayList<>();

    for (final OutputChannel workerOutputChannel : workerOutputChannels.getAllChannels()) {
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      retVal.add(channel);

      resultKeyCollectionWorkers.add(
          new TalariaClusterByStatisticsCollectorWorker(
              workerOutputChannel.getReadableChannel(),
              channel,
              stageDefinition.getFrameReader(),
              clusterBy,
              stageDefinition.createResultKeyStatisticsCollector()
          )
      );
    }

    final ListenableFuture<ClusterByStatisticsCollector> clusterByStatisticsCollectorFuture =
        FrameProcessors.runAllFully(
            Sequences.simple(resultKeyCollectionWorkers),
            exec,
            stageDefinition.createResultKeyStatisticsCollector(),
            ClusterByStatisticsCollector::addAll,
            // Run all result key collection workers simultaneously; they are lightweight and this keeps things moving.
            resultKeyCollectionWorkers.size(),
            Bouncer.unlimited(),
            cancellationId
        );

    // TODO(gianm): extract helper method with other similar code
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

  private static void logKernelStatus(final Collection<WorkerStageKernel> kernels)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Stages: %s",
          kernels
              .stream()
              .sorted(Comparator.comparing(k -> k.getStageDefinition().getStageNumber()))
              .map(k -> StringUtils.format(
                       "S%d:W%d:P[%s]%s:%s:%s",
                       k.getStageDefinition().getStageNumber(),
                       k.getWorkOrder().getWorkerNumber(),
                       StreamSupport.stream(k.getWorkOrder().getInputPartitions().spliterator(), false)
                                    .map(ReadablePartition::getPartitionNumber)
                                    .sorted()
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(",")),
                       k.getStageDefinition().doesShuffle()
                       ? ">" + (k.hasResultPartitionBoundaries() ? k.getResultPartitionBoundaries().size() : "?")
                       : "",
                       k.getStageDefinition().doesShuffle() ? "SHUFFLE" : "RETAIN",
                       k.getPhase()
                   )
              )
              .collect(Collectors.joining("; "))
      );
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

  static class SuperSorterParameters
  {
    private final int numProcessors;
    private final int channelsPerProcessor;

    SuperSorterParameters(int numProcessors, int channelsPerProcessor)
    {
      this.numProcessors = numProcessors;
      this.channelsPerProcessor = channelsPerProcessor;
    }

    static SuperSorterParameters compute(
        final long maxMemory,
        final int numWorkersInJvm,
        final int maxProcessors,
        final int maxSuperSorterProcessors
    )
    {
      final long maxMemoryForFrames =
          (long) (maxMemory * MemoryLimits.FRAME_MEMORY_PERCENT) / numWorkersInJvm;
      final int maxNumFrames = Ints.checkedCast(maxMemoryForFrames / MemoryLimits.FRAME_SIZE);
      final int maxNumFramesForSuperSorter = maxNumFrames - maxProcessors;

      if (maxNumFramesForSuperSorter < MINIMUM_SUPER_SORTER_FRAMES) {
        final long minMemoryNeeded =
            (long) (((long) MemoryLimits.FRAME_SIZE
                     * (MINIMUM_SUPER_SORTER_FRAMES + maxProcessors)
                     * numWorkersInJvm) / MemoryLimits.FRAME_MEMORY_PERCENT);
        throw new ISE(
            "Not enough memory for frames: "
            + "total memory [%,d] (%.02f%% reserved for frames), workers [%,d], processing threads [%,d]; "
            + "minimum memory needed [%,d]. "
            + "Increase memory or decrease workers or processing threads.",
            maxMemory,
            MemoryLimits.FRAME_MEMORY_PERCENT * 100,
            numWorkersInJvm,
            maxProcessors,
            minMemoryNeeded
        );
      }

      final int numSuperSorterProcessors = Math.min(
          maxProcessors,
          Math.min(
              maxNumFramesForSuperSorter / MINIMUM_SUPER_SORTER_FRAMES,
              maxSuperSorterProcessors
          )
      );
      final int channelsPerProcessor = maxNumFramesForSuperSorter / numSuperSorterProcessors - 1;
      return new SuperSorterParameters(numSuperSorterProcessors, channelsPerProcessor);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SuperSorterParameters that = (SuperSorterParameters) o;
      return numProcessors == that.numProcessors && channelsPerProcessor == that.channelsPerProcessor;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(numProcessors, channelsPerProcessor);
    }

    @Override
    public String toString()
    {
      return "SuperSorterParameters{" +
             "numProcessors=" + numProcessors +
             ", channelsPerProcessor=" + channelsPerProcessor +
             '}';
    }
  }
}
