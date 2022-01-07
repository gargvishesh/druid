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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.frame.channel.FrameChannelSequence;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.frame.processor.FrameProcessorExecutor;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.write.HeapMemoryAllocator;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.InsertCannotAllocateSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertCannotBeEmptyFault;
import io.imply.druid.talaria.indexing.error.InsertCannotOrderByDescendingFault;
import io.imply.druid.talaria.indexing.error.InsertCannotReplaceExistingSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertTimeOutOfBoundsFault;
import io.imply.druid.talaria.indexing.error.QueryNotSupportedFault;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyPartitionsFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSinkFrameProcessorFactory;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.ShuffleSpecFactories;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;
import io.imply.druid.talaria.kernel.TargetSizeShuffleSpec;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.kernel.controller.ControllerQueryKernel;
import io.imply.druid.talaria.kernel.controller.ControllerStageKernel;
import io.imply.druid.talaria.kernel.controller.ControllerStagePhase;
import io.imply.druid.talaria.querykit.DataSegmentTimelineView;
import io.imply.druid.talaria.querykit.MultiQueryKit;
import io.imply.druid.talaria.querykit.QueryKit;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.groupby.GroupByPreShuffleFrameProcessorFactory;
import io.imply.druid.talaria.querykit.groupby.GroupByQueryKit;
import io.imply.druid.talaria.querykit.scan.ScanQueryFrameProcessorFactory;
import io.imply.druid.talaria.querykit.scan.ScanQueryKit;
import io.imply.druid.talaria.util.DimensionSchemaUtils;
import io.imply.druid.talaria.util.FutureUtils;
import io.imply.druid.talaria.util.IntervalUtils;
import io.imply.druid.talaria.util.PassthroughAggregatorFactory;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@JsonTypeName(TalariaControllerTask.TYPE)
public class TalariaControllerTask extends AbstractTask implements ChatHandler
{
  public static final String TYPE = "talaria0";
  public static final String DUMMY_DATASOURCE_FOR_SELECT = "__you_have_been_visited_by_talaria";
  private static final Logger log = new Logger(TalariaControllerTask.class);

  private final TalariaQuerySpec querySpec;

  // Enables users, and the web console, to see the original SQL query (if any). Not used by anything else in Druid.
  @Nullable
  private final String sqlQuery;

  // Enables users, and the web console, to see the original SQL context (if any). Not used by any other Druid logic.
  @Nullable
  private final Map<String, Object> sqlQueryContext;

  // Enables users, and the web console, to see the original SQL type names (if any). Not used by any other Druid logic.
  @Nullable
  private final List<String> sqlTypeNames;

  // TODO(gianm): ArrayBlockingQueue; limit size; reasonable errors on overflow
  private final BlockingQueue<Consumer<ControllerQueryKernel>> kernelManipulationQueue = new LinkedBlockingDeque<>();

  // For system error reporting. This is the very first error we got from a worker. (We only report that one.)
  private final AtomicReference<TalariaErrorReport> workerErrorRef = new AtomicReference<>();

  // For live reports.
  private final AtomicReference<QueryDefinition> queryDefRef = new AtomicReference<>();

  // For live reports. task ID -> last reported counters snapshot
  private final ConcurrentHashMap<String, TalariaCountersSnapshot> taskCountersForLiveReports = new ConcurrentHashMap<>();

  // For live reports. stage number -> stage phase
  private final ConcurrentHashMap<Integer, ControllerStagePhase> stagePhasesForLiveReports = new ConcurrentHashMap<>();

  // For live reports. stage number -> runtime interval. Endpoint is eternity's end if the stage is still running.
  private final ConcurrentHashMap<Integer, Interval> stageRuntimesForLiveReports = new ConcurrentHashMap<>();

  private volatile TaskToolbox toolbox;
  private volatile DruidNode selfDruidNode;
  private volatile TalariaWorkerTaskLauncher workerTaskLauncher;
  private volatile TalariaTaskClient netClient;

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector = null;

  @JsonCreator
  public TalariaControllerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") TalariaQuerySpec querySpec,
      @JsonProperty("sqlQuery") @Nullable String sqlQuery,
      @JsonProperty("sqlQueryContext") @Nullable Map<String, Object> sqlQueryContext,
      @JsonProperty("sqlTypeNames") @Nullable List<String> sqlTypeNames,
      @JsonProperty("context") @Nullable Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, getDataSourceForTaskMetadata(Preconditions.checkNotNull(querySpec, "querySpec"))),
        id,
        null,
        getDataSourceForTaskMetadata(querySpec),
        context
    );

    this.querySpec = querySpec;
    this.sqlQuery = sqlQuery;
    this.sqlQueryContext = sqlQueryContext;
    this.sqlTypeNames = sqlTypeNames;

    addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty("spec")
  public TalariaQuerySpec getQuerySpec()
  {
    return querySpec;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSqlQuery()
  {
    return sqlQuery;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Object> getSqlQueryContext()
  {
    return sqlQueryContext;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    // If we're in replace mode, acquire locks for all intervals before declaring the task ready.
    if (isIngestion(querySpec) && ((DataSourceTalariaDestination) querySpec.getDestination()).isReplaceTimeChunks()) {
      final List<Interval> intervals =
          ((DataSourceTalariaDestination) querySpec.getDestination()).getReplaceTimeChunks();

      for (final Interval interval : intervals) {
        final TaskLock taskLock =
            taskActionClient.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval));

        if (taskLock == null) {
          return false;
        } else if (taskLock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked", interval));
        }
      }
    }

    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final Closer closer = Closer.create();
    final TaskStatus retVal;

    try {
      retVal = runTask(toolbox, closer);
    }
    catch (Throwable e) {
      try {
        closer.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      return TaskStatus.failure(getId(), e.toString());
    }

    closer.close();
    return retVal;
  }

  public TaskStatus runTask(final TaskToolbox toolbox, final Closer closer) throws Exception
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<TaskState> workerTaskRunnerFuture = null;
    TalariaCountersSnapshot countersSnapshot = null;
    Yielder<Object[]> resultsYielder = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final TalariaErrorReport errorForReport;

    try {
      queryDef = initializeState(toolbox, closer);

      final Pair<ControllerQueryKernel, ListenableFuture<TaskState>> queryRunResult =
          runQueryUntilDone(queryDef, closer);

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      countersSnapshot = getFinalCountersSnapshot(queryKernel);
      resultsYielder = getFinalResultsYielder(queryDef, queryKernel);
      publishSegmentsIfNeeded(queryDef, queryKernel);
    }
    catch (Throwable e) {
      exceptionEncountered = e;
    }
    finally {
      // Write report even if something went wrong.
      final List<TaskReport> reports = new ArrayList<>();

      if (queryDef != null && countersSnapshot != null) {
        final Map<Integer, ControllerStagePhase> stagePhaseMap =
            queryKernel.getActiveStageKernels()
                       .stream()
                       .collect(
                           Collectors.toMap(
                               stageKernel -> stageKernel.getStageDefinition().getStageNumber(),
                               ControllerStageKernel::getPhase
                           )
                       );

        reports.add(
            makeStageTaskReport(getId(), queryDef, stagePhaseMap, stageRuntimesForLiveReports, countersSnapshot)
        );
      }

      if (resultsYielder != null) {
        reports.add(
            makeResultsTaskReport(getId(), queryDef, resultsYielder, querySpec.getColumnMappings(), sqlTypeNames)
        );
      }

      // There are cases where this is SUCCESS, but we exit with TaskStatus FAILED. This can happen because we need
      // to write out the task report *before* doing final cleanup, and something might go wrong with final cleanup.
      if (queryKernel != null && queryKernel.isSuccess() && exceptionEncountered == null) {
        taskStateForReport = TaskState.SUCCESS;
        errorForReport = null;
      } else {
        taskStateForReport = TaskState.FAILED;

        if (exceptionEncountered != null) {
          // Controller kernel loop threw an exception.
          errorForReport = TalariaErrorReport.fromException(
              getId(),
              TalariaTasks.getHostFromSelfNode(selfDruidNode),
              null,
              exceptionEncountered
          );
        } else if (workerErrorRef.get() != null) {
          // Look for an error reported by a worker.
          errorForReport = workerErrorRef.get();
        } else {
          // Query failed with no explanation.
          errorForReport = TalariaErrorReport.fromFault(
              getId(),
              TalariaTasks.getHostFromSelfNode(selfDruidNode),
              null,
              UnknownFault.INSTANCE
          );
        }
      }

      reports.add(makeStatusTaskReport(getId(), taskStateForReport, errorForReport));

      toolbox.getTaskReportFileWriter().write(
          getId(),
          TaskReport.buildTaskReports(reports.toArray(new TaskReport[0]))
      );
    }

    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage the tasks to exit. Only do this if the query kernel exists and is successful. We
      // assume that if the query kernel is *not* successful, it's because some worker task failed. In that case, the
      // workerTaskRunner will handle canceling all the other worker tasks.
      postFinishToAllTasks();
    }

    // Wait for worker tasks to exit. Ignore their return status. At this point, we've done everything we need to do,
    // so we don't care anymore.
    if (workerTaskRunnerFuture != null) {
      workerTaskRunnerFuture.get();
    }

    if (taskStateForReport == TaskState.SUCCESS) {
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId(), errorForReport.getFault().getCodeWithMessage());
    }
  }

  @Override
  public void stopGracefully(final TaskConfig taskConfig)
  {
    // TODO(gianm): Not very graceful.
    workerTaskLauncher.stop();
    kernelManipulationQueue.add(
        kernel -> {
          throw new TalariaException(CanceledFault.INSTANCE);
        }
    );
  }

  private QueryDefinition initializeState(final TaskToolbox toolbox, final Closer closer)
  {
    this.toolbox = toolbox;
    this.selfDruidNode = injector.getInstance(Key.get(DruidNode.class, Self.class));
    toolbox.getChatHandlerProvider().register(getId(), this, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(getId()));

    this.netClient = makeTaskClient();
    closer.register(netClient::close);

    this.workerTaskLauncher = new TalariaWorkerTaskLauncher(
        getId(),
        getDataSource(),
        toolbox.getIndexingServiceClient(),
        getTuningConfig().getMaxNumConcurrentSubTasks()
    );

    final QueryDefinition queryDef = makeQueryDefinition(
        getId(),
        makeQueryControllerToolKit(),
        makeDataSegmentTimelineView(),
        querySpec
    );

    validateQueryDef(queryDef);

    queryDefRef.set(queryDef);

    return queryDef;
  }

  private Pair<ControllerQueryKernel, ListenableFuture<TaskState>> runQueryUntilDone(
      final QueryDefinition queryDef,
      final Closer closer
  ) throws Exception
  {
    // Start tasks.
    log.debug("Query [%s] starting tasks.", queryDef.getQueryId());

    final ListenableFuture<TaskState> workerTaskLauncherFuture = workerTaskLauncher.start();
    closer.register(workerTaskLauncher::stop);

    workerTaskLauncherFuture.addListener(
        () ->
            kernelManipulationQueue.add(queryKernel -> {
              final TaskState state = FutureUtils.getUncheckedImmediately(workerTaskLauncherFuture);

              if (state == TaskState.FAILED) {
                queryKernel.getActiveStageKernels().forEach(ControllerStageKernel::fail);
              }
            }),
        Execs.directExecutor()
    );

    // Segments to generate; used for making stage-two workers.
    List<SegmentIdWithShardSpec> segmentsToGenerate = null;

    // Track which stages have got their partition boundaries sent out yet.
    final Set<StageId> stageResultPartitionBoundariesSent = new HashSet<>();

    // Start query tracking loop.
    log.debug("Query [%s] starting tracker.", queryDef.getQueryId());
    final ControllerQueryKernel queryKernel = new ControllerQueryKernel(queryDef);

    while (!queryKernel.isDone()) {
      // Start stages that need to be started.
      logKernelStatus(queryDef.getQueryId(), queryKernel);
      for (final ControllerStageKernel stageKernel : queryKernel.getNewStageKernels()) {
        final StageId stageId = stageKernel.getStageDefinition().getId();
        stageKernel.start();

        // Allocate segments, if this is the final stage of an ingestion.
        if (isIngestion(querySpec) && stageId.getStageNumber() == queryDef.getFinalStageDefinition().getStageNumber()) {
          // We need to find the shuffle details (like partition ranges) to generate segments. Generally this is
          // going to correspond to the stage immediately prior to the final segment-generator stage.
          ControllerStageKernel shuffleStageKernel = queryKernel.getStageKernel(
              Iterables.getOnlyElement(queryDef.getFinalStageDefinition().getInputStageIds())
          );

          // TODO(gianm): But sometimes it won't! for example, queries that end in GROUP BY with no ORDER BY
          //    (the final stage is not a shuffle.) The below logic supports this, but it isn't necessarily
          //    always OK! It assumes that stages without shuffling will retain the partition boundaries and
          //    signature of the prior stages, which isn't necessarily guaranteed. (although I think it's true
          //    for QueryKit-generated queries.)
          while (!shuffleStageKernel.getStageDefinition().doesShuffle()) {
            final StageId priorStageId =
                Iterables.getOnlyElement(shuffleStageKernel.getStageDefinition().getInputStageIds());
            shuffleStageKernel = queryKernel.getStageKernel(priorStageId);
          }

          final boolean isTimeBucketed = isTimeBucketedIngestion(querySpec);
          final ClusterByPartitions partitionBoundaries = shuffleStageKernel.getResultPartitionBoundaries();

          if (isTimeBucketed && partitionBoundaries.equals(ClusterByPartitions.oneUniversalPartition())) {
            // TODO(gianm): Properly handle the case where there is no data and remove EmptyInsertFault.
            // TODO(gianm): This also happens when there is a LIMIT, because it obscures the order-by.
            // TODO(gianm): However validation in the Broker SQL layer prevents this
            throw new TalariaException(new InsertCannotBeEmptyFault(getDataSource()));
          } else {
            log.info("Query [%s] generating %d segments.", queryDef.getQueryId(), partitionBoundaries.size());
          }

          segmentsToGenerate = generateSegmentIdsWithShardSpecs(
              (DataSourceTalariaDestination) querySpec.getDestination(),
              shuffleStageKernel.getStageDefinition().getSignature(),
              shuffleStageKernel.getStageDefinition().getShuffleSpec().get().getClusterBy(),
              partitionBoundaries,
              shuffleStageKernel.collectorEncounteredAnyMultiValueField()
          );
        }

        log.info(
            "Query [%s] starting %d workers for stage %d.",
            stageId.getQueryId(),
            stageKernel.getWorkerInputs().size(),
            stageId.getStageNumber()
        );

        stageRuntimesForLiveReports.put(stageId.getStageNumber(), new Interval(DateTimes.nowUtc(), DateTimes.MAX));
        startWorkForStage(queryDef, queryKernel, stageId.getStageNumber(), segmentsToGenerate);
      }

      // Send partition boundaries to tasks, if the time is right.
      logKernelStatus(queryDef.getQueryId(), queryKernel);
      for (final ControllerStageKernel stageKernel : queryKernel.getActiveStageKernels()) {
        final StageId stageId = stageKernel.getStageDefinition().getId();

        if (stageKernel.getStageDefinition().mustGatherResultKeyStatistics()
            && stageKernel.hasResultPartitions()
            && stageResultPartitionBoundariesSent.add(stageId)) {
          if (log.isDebugEnabled()) {
            final ClusterByPartitions partitions = stageKernel.getResultPartitionBoundaries();
            log.debug(
                "Query [%s] sending out partition boundaries for stage %d: %s",
                stageId.getQueryId(),
                stageId.getStageNumber(),
                IntStream.range(0, partitions.size())
                         .mapToObj(i -> StringUtils.format("%s:%s", i, partitions.get(i)))
                         .collect(Collectors.joining(", "))
            );
          } else {
            log.info(
                "Query [%s] sending out partition boundaries for stage %d.",
                stageId.getQueryId(),
                stageId.getStageNumber()
            );
          }

          postResultPartitionBoundariesForStage(
              queryDef,
              stageId.getStageNumber(),
              stageKernel.getResultPartitionBoundaries(),
              stageKernel.getWorkerInputs().size()
          );
        }
      }

      logKernelStatus(queryDef.getQueryId(), queryKernel);

      // Live reports: update stage phases.
      for (ControllerStageKernel stageKernel : queryKernel.getActiveStageKernels()) {
        stagePhasesForLiveReports.put(stageKernel.getStageDefinition().getStageNumber(), stageKernel.getPhase());
      }

      // Live reports: update stage end times for any stages that just ended.
      for (ControllerStageKernel stageKernel : queryKernel.getActiveStageKernels()) {
        if (stageKernel.getPhase() == ControllerStagePhase.RESULTS_COMPLETE) {
          stageRuntimesForLiveReports.compute(
              stageKernel.getStageDefinition().getStageNumber(),
              (k, currentValue) -> {
                if (currentValue.getEnd().equals(DateTimes.MAX)) {
                  return new Interval(currentValue.getStart(), DateTimes.nowUtc());
                } else {
                  return currentValue;
                }
              }
          );
        }
      }

      if (!queryKernel.isDone()) {
        // Wait for next command, run it, then look through the query tracker again.
        kernelManipulationQueue.take().accept(queryKernel);
      }
    }

    if (!queryKernel.isSuccess()) {
      // Look for a known failure reason and throw a meaningful exception.
      for (final ControllerStageKernel stageKernel : queryKernel.getActiveStageKernels()) {
        if (stageKernel.getPhase() == ControllerStagePhase.FAILED) {
          switch (stageKernel.getFailureReason()) {
            case TOO_MANY_PARTITIONS:
              throw new TalariaException(
                  new TooManyPartitionsFault(stageKernel.getStageDefinition().getMaxPartitionCount())
              );

            default:
              // Fall through.
          }
        }
      }
    }

    return Pair.of(queryKernel, workerTaskLauncherFuture);
  }

  private ParallelIndexTuningConfig getTuningConfig()
  {
    return querySpec.getTuningConfig();
  }

  /**
   * Used by subtasks to post {@link ClusterByStatisticsSnapshot} for shuffling stages.
   *
   * See {@link TalariaTaskClient#postKeyStatistics} for the client-side code that calls this API.
   */
  @POST
  @Path("/keyStatistics/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostKeyStatistics(
      final Object keyStatisticsObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    kernelManipulationQueue.add(
        queryKernel -> {
          // TODO(gianm): don't blow up with NPE if given nonexistent stageid
          final ControllerStageKernel kernel = queryKernel.getStageKernel(stageNumber);

          // We need a specially-decorated ObjectMapper to deserialize key statistics.
          final StageDefinition stageDef = kernel.getStageDefinition();
          final ObjectMapper mapper = TalariaTasks.decorateObjectMapperForClusterByKey(
              toolbox.getJsonMapper(),
              stageDef.getSignature(),
              stageDef.getShuffleSpec().get().getClusterBy(),
              stageDef.getShuffleSpec().get().doesAggregateByClusterKey()
          );

          // TODO(gianm): do something useful if this conversion fails
          final ClusterByStatisticsSnapshot keyStatistics =
              mapper.convertValue(keyStatisticsObject, ClusterByStatisticsSnapshot.class);

          kernel.addResultKeyStatisticsForWorker(workerNumber, keyStatistics);
        }
    );

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post system errors. Note that the errors are organized by taskId, not by query/stage/worker,
   * because system errors are associated with a task rather than a specific query/stage/worker execution context.
   *
   * See {@link TalariaTaskClient#postWorkerError} for the client-side code that calls this API.
   */
  @POST
  @Path("/workerError/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostWorkerError(
      final TalariaErrorReport errorReport,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    workerErrorRef.compareAndSet(null, errorReport);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post {@link TalariaCountersSnapshot} periodically.
   *
   * See {@link TalariaTaskClient#postCounters} for the client-side code that calls this API.
   */
  @POST
  @Path("/counters/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostCounters(
      final TalariaCountersSnapshot countersSnapshot,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    // TODO(gianm): expire old counters when tasks fail
    taskCountersForLiveReports.put(taskId, countersSnapshot);

    return Response.status(Response.Status.OK).build();
  }

  /**
   * Used by subtasks to post notifications that their results are ready.
   *
   * See {@link TalariaTaskClient#postResultsComplete} for the client-side code that calls this API.
   */
  @POST
  @Path("/resultsComplete/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostResultsComplete(
      final Object resultObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    kernelManipulationQueue.add(
        queryKernel -> {
          // TODO(gianm): don't blow up with NPE if given nonexistent stageid
          final ControllerStageKernel kernel = queryKernel.getStageKernel(new StageId(queryId, stageNumber));

          // TODO(gianm): do something useful if this conversion fails
          //noinspection unchecked
          final Object convertedResultObject = toolbox.getJsonMapper().convertValue(
              resultObject,
              kernel.getStageDefinition().getProcessorFactory().getAccumulatedResultTypeReference()
          );

          kernel.setResultsCompleteForWorker(workerNumber, convertedResultObject);
        }
    );

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link TalariaTaskClient#getTaskList} for the client-side code that calls this API.
   */
  @GET
  @Path("/taskList")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetTaskList(@Context final HttpServletRequest req)
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    return Response.ok(new TalariaTaskList(getTaskIds().orElse(null))).build();
  }

  /**
   * See {@link org.apache.druid.indexing.overlord.RemoteTaskRunner#streamTaskReports} for the client-side code that
   * calls this API.
   */
  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetLiveReports(@Context final HttpServletRequest req)
  {
    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task is not running yet").build();
    }

    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), toolbox.getAuthorizerMapper());

    final QueryDefinition queryDef = queryDefRef.get();

    if (queryDef == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    final TalariaCountersSnapshot snapshot = makeCountersSnapshotForLiveReports();

    final Map<String, TaskReport> reports = TaskReport.buildTaskReports(
        makeStageTaskReport(
            getId(),
            queryDef,
            stagePhasesForLiveReports,
            stageRuntimesForLiveReports,
            snapshot
        ),
        makeStatusTaskReport(getId(), TaskState.RUNNING, null)
    );

    return Response.ok(reports).build();
  }

  /**
   * Returns the segments that will be generated by this job. Delegates to
   * {@link #generateSegmentIdsWithShardSpecsForAppend} or {@link #generateSegmentIdsWithShardSpecsForReplace} as
   * appropriate. This is a potentially expensive call, since it requires calling Overlord APIs.
   *
   * @throws TalariaException with {@link InsertCannotAllocateSegmentFault} if an allocation cannot be made
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecs(
      final DataSourceTalariaDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean clusterByEncounteredMultiValuedFields
  ) throws IOException
  {
    if (destination.isReplaceTimeChunks()) {
      return generateSegmentIdsWithShardSpecsForReplace(
          destination,
          signature,
          clusterBy,
          partitionBoundaries,
          clusterByEncounteredMultiValuedFields
      );
    } else {
      return generateSegmentIdsWithShardSpecsForAppend(destination, partitionBoundaries);
    }
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecsForAppend(
      final DataSourceTalariaDestination destination,
      final ClusterByPartitions partitionBoundaries
  ) throws IOException
  {
    final Granularity segmentGranularity = destination.getSegmentGranularity();

    String previousSegmentId = null;

    final List<SegmentIdWithShardSpec> retVal = new ArrayList<>(partitionBoundaries.size());

    for (ClusterByPartition partitionBoundary : partitionBoundaries) {
      final DateTime timestamp = getBucketDateTime(partitionBoundary, segmentGranularity);
      final SegmentIdWithShardSpec allocation = toolbox.getTaskActionClient().submit(
          new SegmentAllocateAction(
              getDataSource(),
              timestamp,
              // Same granularity for queryGranularity, segmentGranularity because we don't have insight here
              // into what queryGranularity "actually" is. (It depends on what time floor function was used.)
              segmentGranularity,
              segmentGranularity,
              getId(),
              previousSegmentId,
              false,
              NumberedPartialShardSpec.instance(),
              LockGranularity.TIME_CHUNK,
              TaskLockType.SHARED
          )
      );

      if (allocation == null) {
        throw new TalariaException(
            new InsertCannotAllocateSegmentFault(
                getDataSource(),
                segmentGranularity.bucket(timestamp)
            )
        );
      }

      retVal.add(allocation);
      previousSegmentId = allocation.asSegmentId().toString();
    }

    return retVal;
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecsForReplace(
      final DataSourceTalariaDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean clusterByEncounteredMultiValuedFields
  ) throws IOException
  {
    final SegmentIdWithShardSpec[] retVal = new SegmentIdWithShardSpec[partitionBoundaries.size()];
    final Granularity segmentGranularity = destination.getSegmentGranularity();
    final List<String> shardColumns;

    if (clusterByEncounteredMultiValuedFields) {
      // DimensionRangeShardSpec cannot handle multi-valued fields.
      shardColumns = Collections.emptyList();
    } else {
      shardColumns = computeShardColumns(signature, clusterBy, querySpec.getColumnMappings());
    }

    // Group partition ranges by bucket (time chunk), so we can generate shardSpecs for each bucket independently.
    final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket = new HashMap<>();
    for (int i = 0; i < partitionBoundaries.ranges().size(); i++) {
      ClusterByPartition partitionBoundary = partitionBoundaries.ranges().get(i);
      final DateTime bucketDateTime = getBucketDateTime(partitionBoundary, segmentGranularity);
      partitionsByBucket.computeIfAbsent(bucketDateTime, ignored -> new ArrayList<>())
                        .add(Pair.of(i, partitionBoundary));
    }

    // Process buckets (time chunks) one at a time.
    for (final Map.Entry<DateTime, List<Pair<Integer, ClusterByPartition>>> bucketEntry : partitionsByBucket.entrySet()) {
      final Interval interval = segmentGranularity.bucket(bucketEntry.getKey());

      // Validate interval against the replaceTimeChunks set of intervals.
      if (destination.getReplaceTimeChunks().stream().noneMatch(chunk -> chunk.contains(interval))) {
        throw new TalariaException(new InsertTimeOutOfBoundsFault(interval));
      }

      final List<Pair<Integer, ClusterByPartition>> ranges = bucketEntry.getValue();
      String version = null;

      final List<TaskLock> locks = toolbox.getTaskActionClient().submit(new LockListAction());
      for (final TaskLock lock : locks) {
        if (lock.getInterval().contains(interval)) {
          version = lock.getVersion();
        }
      }

      if (version == null) {
        // Lock was revoked, probably, because we should have originally acquired it in isReady.
        throw new TalariaException(new InsertCannotAllocateSegmentFault(getDataSource(), interval));
      }

      for (int segmentNumber = 0; segmentNumber < ranges.size(); segmentNumber++) {
        final int partitionNumber = ranges.get(segmentNumber).lhs;
        final ShardSpec shardSpec;

        if (shardColumns.isEmpty()) {
          shardSpec = new NumberedShardSpec(segmentNumber, ranges.size());
        } else {
          final ClusterByPartition range = ranges.get(segmentNumber).rhs;
          final StringTuple start =
              segmentNumber == 0 ? null : makeStringTuple(clusterBy, range.getStart());
          final StringTuple end =
              segmentNumber == ranges.size() - 1 ? null : makeStringTuple(clusterBy, range.getEnd());

          shardSpec = new DimensionRangeShardSpec(shardColumns, start, end, segmentNumber, ranges.size());
        }

        retVal[partitionNumber] = new SegmentIdWithShardSpec(getDataSource(), interval, version, shardSpec);
      }
    }

    return Arrays.asList(retVal);
  }

  /**
   * Returns a complete list of task ids, ordered by worker number. The Nth task has worker number N.
   *
   * If the currently-running set of tasks is incomplete, returns an absent Optional.
   */
  private Optional<List<String>> getTaskIds()
  {
    if (workerTaskLauncher == null) {
      return Optional.empty();
    }

    return workerTaskLauncher.getTaskList().map(TalariaTaskList::getTaskIds);
  }

  @Nullable
  private List<Object> makeWorkerFactoryInfosForStage(
      final QueryDefinition queryDef,
      final int stageNumber,
      final List<ReadablePartitions> workerInputs,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    if (isIngestion(querySpec) && stageNumber == queryDef.getFinalStageDefinition().getStageNumber()) {
      // noinspection unchecked,rawtypes
      return (List) makeSegmentGeneratorWorkerFactoryInfos(workerInputs, segmentsToGenerate);
    } else {
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  private QueryKit makeQueryControllerToolKit()
  {
    final Map<Class<? extends Query>, QueryKit> kitMap =
        ImmutableMap.<Class<? extends Query>, QueryKit>builder()
                    .put(ScanQuery.class, new ScanQueryKit(toolbox.getJsonMapper()))
                    .put(GroupByQuery.class, new GroupByQueryKit())
                    .build();

    return new MultiQueryKit(kitMap);
  }

  private DataSegmentTimelineView makeDataSegmentTimelineView()
  {
    return (dataSource, intervals) -> {
      final Collection<DataSegment> dataSegments =
          toolbox.getCoordinatorClient().fetchUsedSegmentsInDataSourceForIntervals(dataSource, intervals);

      if (dataSegments.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(VersionedIntervalTimeline.forSegments(dataSegments));
      }
    };
  }

  private List<List<SegmentIdWithShardSpec>> makeSegmentGeneratorWorkerFactoryInfos(
      final List<ReadablePartitions> workerInputs,
      final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    return workerInputs.stream().map(
        readablePartitions ->
            StreamSupport.stream(readablePartitions.spliterator(), false)
                         .map(rp -> segmentsToGenerate.get(rp.getPartitionNumber()))
                         .collect(Collectors.toList())
    ).collect(Collectors.toList());
  }

  private void contactWorkersForStage(final TaskContactFn contactFn, final int numWorkers)
  {
    // TODO(gianm): Better handling for task failures
    final List<String> taskIds = getTaskIds().orElseThrow(() -> new ISE("Tasks went away!"));

    for (int workerNumber = 0; workerNumber < numWorkers; workerNumber++) {
      final String taskId = taskIds.get(workerNumber);

      contactFn.contactTask(netClient, taskId, workerNumber);
    }
  }

  private void startWorkForStage(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel,
      final int stageNumber,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final List<Object> extraInfos = makeWorkerFactoryInfosForStage(
        queryDef,
        stageNumber,
        queryKernel.getStageKernel(stageNumber).getWorkerInputs(),
        segmentsToGenerate
    );

    final List<WorkOrder> workOrders = queryKernel.createWorkOrders(stageNumber, extraInfos);

    contactWorkersForStage(
        (netClient, taskId, workerNumber) -> netClient.postWorkOrder(taskId, workOrders.get(workerNumber)),
        workOrders.size()
    );
  }

  private void postResultPartitionBoundariesForStage(
      final QueryDefinition queryDef,
      final int stageNumber,
      final ClusterByPartitions resultPartitionBoundaries,
      final int numWorkers
  )
  {
    contactWorkersForStage(
        (netClient, taskId, workerNumber) ->
            netClient.postResultPartitionBoundaries(
                taskId,
                new StageId(queryDef.getQueryId(), stageNumber),
                resultPartitionBoundaries
            ),
        numWorkers
    );
  }

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceTalariaDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   *
   * If any existing segments cannot be dropped because their intervals are not wholly contained within the
   * replacement parameter, throws a {@link TalariaException} with {@link InsertCannotReplaceExistingSegmentFault}.
   */
  private void publishAllSegments(final Set<DataSegment> segments) throws IOException
  {
    final DataSourceTalariaDestination destination = (DataSourceTalariaDestination) querySpec.getDestination();
    final Set<DataSegment> segmentsToDrop;

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (intervalsToDrop.isEmpty()) {
        segmentsToDrop = null;
      } else {
        segmentsToDrop =
            ImmutableSet.copyOf(
                toolbox.getTaskActionClient().submit(
                    new RetrieveUsedSegmentsAction(
                        getDataSource(),
                        null,
                        intervalsToDrop,
                        Segments.ONLY_VISIBLE
                    )
                )
            );

        // Validate that there are no segments that partially overlap the intervals-to-drop. Otherwise, the replace
        // may be incomplete.
        for (final DataSegment segmentToDrop : segmentsToDrop) {
          if (destination.getReplaceTimeChunks()
                         .stream()
                         .noneMatch(interval -> interval.contains(segmentToDrop.getInterval()))) {
            throw new TalariaException(new InsertCannotReplaceExistingSegmentFault(segmentToDrop.getId()));
          }
        }
      }

      if (segments.isEmpty()) {
        // Nothing to publish, only drop. We already validated that the intervalsToDrop do not have any
        // partially-overlapping segments, so it's safe to drop them as intervals instead of as specific segments.
        // TODO(gianm): Make this transactional
        for (final Interval interval : intervalsToDrop) {
          toolbox.getTaskActionClient()
                 .submit(new MarkSegmentsAsUnusedAction(getDataSource(), interval));
        }
      } else {
        toolbox.getTaskActionClient()
               .submit(SegmentTransactionalInsertAction.overwriteAction(null, segmentsToDrop, segments));
      }
    } else if (!segments.isEmpty()) {
      // Append mode.
      toolbox.getTaskActionClient().submit(new SegmentInsertAction(segments));
    }
  }

  /**
   * When doing an ingestion with {@link DataSourceTalariaDestination#isReplaceTimeChunks()}, finds intervals
   * containing data that should be dropped.
   */
  private List<Interval> findIntervalsToDrop(final Set<DataSegment> publishedSegments)
  {
    // Safe to cast because publishAllSegments is only called for dataSource destinations.
    final DataSourceTalariaDestination destination = (DataSourceTalariaDestination) querySpec.getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  private TalariaCountersSnapshot getCountersFromAllTasks()
  {
    final TalariaCounters counters = new TalariaCounters();

    workerTaskLauncher.getTaskList().ifPresent(
        taskList -> {
          for (String taskId : taskList.getTaskIds()) {
            // TODO(gianm): handle duplication/retries when updating counters (attempt #? task id?)
            final TalariaCountersSnapshot snapshot = netClient.getCounters(taskId);
            counters.addAll(snapshot);
          }
        }
    );

    return counters.snapshot();
  }

  private void postFinishToAllTasks()
  {
    workerTaskLauncher.getTaskList().ifPresent(
        taskList -> {
          for (String taskId : taskList.getTaskIds()) {
            netClient.postFinish(taskId);
          }
        }
    );
  }

  private TalariaTaskClient makeTaskClient()
  {
    return new TalariaTaskClient(
        injector.getInstance(Key.get(HttpClient.class, EscalatedGlobal.class)),
        toolbox.getJsonMapper(),
        new ClientBasedTaskInfoProvider(toolbox.getIndexingServiceClient()),
        getTuningConfig().getChatHandlerTimeout(),
        getId(),
        getTuningConfig().getChatHandlerNumRetries()
    );
  }

  private TalariaCountersSnapshot makeCountersSnapshotForLiveReports()
  {
    final TalariaCounters counters = new TalariaCounters();

    for (final TalariaCountersSnapshot snapshot : taskCountersForLiveReports.values()) {
      counters.addAll(snapshot);
    }

    return counters.snapshot();
  }

  private TalariaCountersSnapshot getFinalCountersSnapshot(final ControllerQueryKernel queryKernel)
  {
    if (queryKernel.isSuccess()) {
      return getCountersFromAllTasks();
    } else {
      return makeCountersSnapshotForLiveReports();
    }
  }

  @Nullable
  private Yielder<Object[]> getFinalResultsYielder(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  )
  {
    if (queryKernel.isSuccess() && isInlineResults(querySpec)) {
      final ControllerStageKernel finalStageKernel =
          queryKernel.getStageKernel(queryDef.getFinalStageDefinition().getStageNumber());

      final List<String> taskIds = getTaskIds().get();

      final ListeningExecutorService resultReaderExec =
          MoreExecutors.listeningDecorator(Execs.singleThreaded("result-reader-%d"));

      // TODO(gianm): something sane with allocator?
      final InputChannels inputChannels = InputChannels.create(
          queryDef,
          new int[]{finalStageKernel.getStageDefinition().getStageNumber()},
          finalStageKernel.getResultPartitions(),
          (stageId, workerNumber, partitionNumber) ->
              netClient.getChannelData(taskIds.get(workerNumber), stageId, partitionNumber, resultReaderExec),
          // TODO(gianm): adjust size to make sense
          () -> HeapMemoryAllocator.createWithCapacity(5_000_000L),
          new FrameProcessorExecutor(resultReaderExec),
          null
      );

      return Yielders.each(
          Sequences.concat(
              StreamSupport.stream(finalStageKernel.getResultPartitions().spliterator(), false)
                           .map(
                               readablePartition -> {
                                 try {
                                   return new FrameChannelSequence(
                                       inputChannels.openChannel(
                                           new StagePartition(
                                               finalStageKernel.getStageDefinition().getId(),
                                               readablePartition.getPartitionNumber()
                                           )
                                       )
                                   );
                                 }
                                 catch (IOException e) {
                                   throw new RuntimeException(e);
                                 }
                               }
                           ).collect(Collectors.toList())
          ).flatMap(
              frame -> {
                final Cursor cursor = FrameProcessors.makeCursor(
                    frame,
                    finalStageKernel.getStageDefinition().getFrameReader()
                );

                final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
                final ColumnMappings columnMappings = querySpec.getColumnMappings();
                final List<ColumnValueSelector> selectors =
                    columnMappings.getMappings()
                                  .stream()
                                  .map(
                                      mapping ->
                                          columnSelectorFactory.makeColumnValueSelector(
                                              mapping.getQueryColumn())
                                  ).collect(Collectors.toList());

                final List<Object[]> retVal = new ArrayList<>();
                while (!cursor.isDone()) {
                  final Object[] row = new Object[columnMappings.getMappings().size()];
                  for (int i = 0; i < row.length; i++) {
                    row[i] = selectors.get(i).getObject();
                  }
                  retVal.add(row);
                  cursor.advance();
                }

                return Sequences.simple(retVal);
              }
          ).withBaggage(resultReaderExec::shutdownNow)
      );
    } else {
      return null;
    }
  }

  private void publishSegmentsIfNeeded(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (queryKernel.isSuccess() && isIngestion(querySpec)) {
      final ControllerStageKernel finalStageKernel =
          queryKernel.getStageKernel(queryDef.getFinalStageDefinition().getStageNumber());

      //noinspection unchecked
      final Set<DataSegment> segments = (Set<DataSegment>) finalStageKernel.getResultObject();
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments);
    }
  }

  private void validateQueryDef(final QueryDefinition queryDef)
  {
    validateQueryDefColumnCount(queryDef);
  }

  private void validateQueryDefColumnCount(final QueryDefinition queryDef)
  {
    for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
      final int numColumns = stageDef.getSignature().size();

      if (numColumns > MemoryLimits.FRAME_MAX_COLUMNS) {
        throw new TalariaException(new TooManyColumnsFault(numColumns, MemoryLimits.FRAME_MAX_COLUMNS));
      }
    }
  }

  private static QueryDefinition makeQueryDefinition(
      final String queryId,
      final QueryKit toolKit,
      final DataSegmentTimelineView timelineView,
      final TalariaQuerySpec querySpec
  )
  {
    final ParallelIndexTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ShuffleSpecFactory shuffleSpecFactory;

    if (isIngestion(querySpec)) {
      shuffleSpecFactory = (clusterBy, aggregate) ->
          new TargetSizeShuffleSpec(
              clusterBy,
              tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
              aggregate
          );
    } else if (querySpec.getDestination() instanceof TaskReportTalariaDestination) {
      shuffleSpecFactory = ShuffleSpecFactories.singlePartition();
    } else if (querySpec.getDestination() instanceof ExternalTalariaDestination) {
      shuffleSpecFactory = ShuffleSpecFactories.subQueryWithMaxWorkerCount(tuningConfig.getMaxNumConcurrentSubTasks());
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }

    final QueryDefinition queryDef;

    try {
      //noinspection unchecked
      queryDef = toolKit.makeQueryDefinition(
          queryId,
          querySpec.getQuery(),
          timelineView,
          toolKit,
          shuffleSpecFactory,
          tuningConfig.getMaxNumConcurrentSubTasks(),
          0
      );
    }
    catch (Exception e) {
      throw new TalariaException(e, QueryNotSupportedFault.INSTANCE);
    }

    final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
    final ClusterBy queryClusterBy = queryDef.getClusterByForStage(queryDef.getFinalStageDefinition().getStageNumber());
    final ColumnMappings columnMappings = querySpec.getColumnMappings();

    if (isIngestion(querySpec)) {
      final Pair<List<DimensionSchema>, List<AggregatorFactory>> dimensionsAndAggregators =
          makeDimensionsAndAggregatorsForIngestion(
              querySignature,
              queryClusterBy,
              columnMappings
          );

      final DataSchema dataSchema = new DataSchema(
          ((DataSourceTalariaDestination) querySpec.getDestination()).getDataSource(),
          new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
          new DimensionsSpec(dimensionsAndAggregators.lhs),
          dimensionsAndAggregators.rhs.toArray(new AggregatorFactory[0]),
          new ArbitraryGranularitySpec(Granularities.NONE, false, Intervals.ONLY_ETERNITY),
          new TransformSpec(null, Collections.emptyList())
      );

      return QueryDefinition
          .builder(queryDef)
          .add(
              StageDefinition.builder(queryDef.getNextStageNumber())
                             .inputStages(queryDef.getFinalStageDefinition().getStageNumber())
                             .maxWorkerCount(tuningConfig.getMaxNumConcurrentSubTasks())
                             .processorFactory(
                                 new TalariaSegmentGeneratorFrameProcessorFactory(
                                     dataSchema,
                                     columnMappings,
                                     tuningConfig
                                 )
                             )
          )
          .build();
    } else if (querySpec.getDestination() instanceof ExternalTalariaDestination) {
      return QueryDefinition
          .builder(queryDef)
          .add(
              StageDefinition.builder(queryDef.getNextStageNumber())
                             .inputStages(queryDef.getFinalStageDefinition().getStageNumber())
                             .maxWorkerCount(tuningConfig.getMaxNumConcurrentSubTasks())
                             .processorFactory(new TalariaExternalSinkFrameProcessorFactory(columnMappings))
          )
          .build();
    } else if (querySpec.getDestination() instanceof TaskReportTalariaDestination) {
      return queryDef;
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  private static boolean isIngestion(final TalariaQuerySpec querySpec)
  {
    return querySpec.getDestination() instanceof DataSourceTalariaDestination;
  }

  private static boolean isInlineResults(final TalariaQuerySpec querySpec)
  {
    return querySpec.getDestination() instanceof TaskReportTalariaDestination;
  }

  /**
   * TODO(gianm): Hack, because tasks must be associated with a datasource.
   */
  private static String getDataSourceForTaskMetadata(final TalariaQuerySpec querySpec)
  {
    final TalariaDestination destination = querySpec.getDestination();

    if (destination instanceof DataSourceTalariaDestination) {
      return ((DataSourceTalariaDestination) destination).getDataSource();
    } else {
      return DUMMY_DATASOURCE_FOR_SELECT;
    }
  }

  private static boolean isTimeBucketedIngestion(final TalariaQuerySpec querySpec)
  {
    return isIngestion(querySpec)
           && !((DataSourceTalariaDestination) querySpec.getDestination()).getSegmentGranularity()
                                                                          .equals(Granularities.ALL);
  }

  /**
   * Compute shard columns for {@link DimensionRangeShardSpec}. Returns an empty list if range-based sharding
   * is not applicable.
   */
  private static List<String> computeShardColumns(
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ColumnMappings columnMappings
  )
  {
    final List<ClusterByColumn> clusterByColumns = clusterBy.getColumns();
    final List<String> shardColumns = new ArrayList<>();
    final boolean boosted = isClusterByBoosted(clusterBy);

    if (clusterByColumns.isEmpty() || (boosted && clusterByColumns.size() == 1)) {
      return Collections.emptyList();
    }

    for (int i = clusterBy.getBucketByCount(); i < clusterByColumns.size(); i++) {
      final ClusterByColumn column = clusterByColumns.get(i);
      final List<String> outputColumns = columnMappings.getOutputColumnsForQueryColumn(column.columnName());

      // DimensionRangeShardSpec only handles ascending order.
      if (column.descending()) {
        return Collections.emptyList();
      }

      // DimensionRangeShardSpec only handles strings.
      if (!ColumnType.STRING.equals(signature.getColumnType(column.columnName()).orElse(null))) {
        return Collections.emptyList();
      }

      // DimensionRangeShardSpec only handles columns that appear as-is in the output.
      if (outputColumns.isEmpty()) {
        return Collections.emptyList();
      }

      shardColumns.add(outputColumns.get(0));
    }

    return shardColumns;
  }

  private static boolean isClusterByBoosted(final ClusterBy clusterBy)
  {
    return !clusterBy.getColumns().isEmpty()
           && clusterBy.getColumns()
                       .get(clusterBy.getColumns().size() - 1)
                       .columnName()
                       .equals(QueryKitUtils.PARTITION_BOOST_COLUMN);
  }

  private static StringTuple makeStringTuple(final ClusterBy clusterBy, final ClusterByKey key)
  {
    final String[] array = new String[clusterBy.getColumns().size() - clusterBy.getBucketByCount()];
    final boolean boosted = isClusterByBoosted(clusterBy);

    for (int i = 0; i < array.length; i++) {
      final Object val = key.get(clusterBy.getBucketByCount() + i);

      if (i == array.length - 1 && boosted) {
        // Boost column
        //noinspection RedundantCast: false alarm; the cast is necessary
        array[i] = StringUtils.format("%016d", (long) val);
      } else {
        array[i] = (String) val;
      }
    }

    return new StringTuple(array);
  }

  private static Pair<List<DimensionSchema>, List<AggregatorFactory>> makeDimensionsAndAggregatorsForIngestion(
      final RowSignature querySignature,
      final ClusterBy queryClusterBy,
      final ColumnMappings columnMappings
  )
  {
    final List<DimensionSchema> dimensions = new ArrayList<>();
    final List<AggregatorFactory> aggregators = new ArrayList<>();

    // TODO(gianm): this doesn't work when ordering by something that is not selected!
    final Set<String> outputColumnsInOrder = new LinkedHashSet<>();

    for (final ClusterByColumn clusterByColumn : queryClusterBy.getColumns()) {
      if (clusterByColumn.descending()) {
        throw new TalariaException(new InsertCannotOrderByDescendingFault(clusterByColumn.columnName()));
      }

      outputColumnsInOrder.addAll(columnMappings.getOutputColumnsForQueryColumn(clusterByColumn.columnName()));
    }

    outputColumnsInOrder.addAll(columnMappings.getOutputColumnNames());

    for (final String outputColumn : outputColumnsInOrder) {
      final String queryColumn = columnMappings.getQueryColumnForOutputColumn(outputColumn);
      final ColumnType type =
          querySignature.getColumnType(queryColumn)
                        .orElseThrow(() -> new ISE("No type for column [%s]", outputColumn));

      if (!outputColumn.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        if (type.is(ValueType.COMPLEX)) {
          // TODO(gianm): hack to workaround the fact that aggregators are required for transferring complex types
          aggregators.add(new PassthroughAggregatorFactory(outputColumn, type.getComplexTypeName()));
        } else {
          dimensions.add(DimensionSchemaUtils.createDimensionSchema(outputColumn, type.getType()));
        }
      }
    }

    return Pair.of(dimensions, aggregators);
  }

  private static DateTime getBucketDateTime(
      final ClusterByPartition partitionBoundary,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return DateTimes.utc(0);
    } else {
      final DateTime timestamp = DateTimes.utc((long) partitionBoundary.getStart().get(0));

      if (segmentGranularity.bucketStart(timestamp.getMillis()) != timestamp.getMillis()) {
        // It's a bug in... something? if this happens.
        throw new ISE(
            "Received boundary value [%s] misaligned with segmentGranularity [%s]",
            timestamp,
            segmentGranularity
        );
      }

      return timestamp;
    }
  }

  private static TalariaStagesTaskReport makeStageTaskReport(
      final String taskId,
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final TalariaCountersSnapshot countersSnapshot
  )
  {
    // TODO(gianm): the setup for stageQueryMap is totally a hack; clean up somehow.
    final Map<Integer, Query<?>> stageQueryMap = new HashMap<>();

    for (final StageDefinition stageDefinition : queryDef.getStageDefinitions()) {
      final FrameProcessorFactory processorFactory = stageDefinition.getProcessorFactory();

      if (processorFactory instanceof ScanQueryFrameProcessorFactory) {
        stageQueryMap.put(
            stageDefinition.getStageNumber(),
            ((ScanQueryFrameProcessorFactory) processorFactory).getQuery()
        );
      } else if (processorFactory instanceof GroupByPreShuffleFrameProcessorFactory) {
        stageQueryMap.put(
            stageDefinition.getStageNumber(),
            ((GroupByPreShuffleFrameProcessorFactory) processorFactory).getQuery()
        );
      }
    }

    return new TalariaStagesTaskReport(
        taskId,
        TalariaStagesTaskReportPayload.create(
            queryDef,
            ImmutableMap.copyOf(stagePhaseMap),
            copyOfStageRuntimesEndingAtCurrentTime(stageRuntimeMap),
            stageQueryMap,
            countersSnapshot
        )
    );
  }

  private static TalariaResultsTaskReport makeResultsTaskReport(
      final String taskId,
      final QueryDefinition queryDef,
      final Yielder<Object[]> resultsYielder,
      final ColumnMappings columnMappings,
      @Nullable final List<String> sqlTypeNames
  )
  {
    final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
    final RowSignature.Builder mappedSignature = RowSignature.builder();

    for (final ColumnMapping mapping : columnMappings.getMappings()) {
      mappedSignature.add(
          mapping.getOutputColumn(),
          querySignature.getColumnType(mapping.getQueryColumn()).orElse(null)
      );
    }

    return new TalariaResultsTaskReport(
        taskId,
        new TalariaResultsTaskReportPayload(mappedSignature.build(), sqlTypeNames, resultsYielder)
    );
  }

  private static TalariaStatusTaskReport makeStatusTaskReport(
      final String taskId,
      final TaskState taskState,
      @Nullable final TalariaErrorReport errorReport
  )
  {
    return new TalariaStatusTaskReport(
        taskId,
        new TalariaStatusTaskReportPayload(taskState, errorReport)
    );
  }

  private static Map<Integer, Interval> copyOfStageRuntimesEndingAtCurrentTime(
      final Map<Integer, Interval> stageRuntimesMap
  )
  {
    final Int2ObjectMap<Interval> retVal = new Int2ObjectOpenHashMap<>(stageRuntimesMap.size());
    final DateTime now = DateTimes.nowUtc();

    for (Map.Entry<Integer, Interval> entry : stageRuntimesMap.entrySet()) {
      final int stageNumber = entry.getKey();
      final Interval interval = entry.getValue();

      retVal.put(
          stageNumber,
          interval.getEnd().equals(DateTimes.MAX) ? new Interval(interval.getStart(), now) : interval
      );
    }

    return retVal;
  }

  private static void logKernelStatus(final String queryId, final ControllerQueryKernel queryKernel)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Query [%s] kernel state: %s",
          queryId,
          queryKernel.getActiveStageKernels()
                     .stream()
                     .sorted(Comparator.comparing(k -> k.getStageDefinition().getStageNumber()))
                     .map(k -> StringUtils.format(
                              "%d:%d[%s:%s]:%d>%d",
                              k.getStageDefinition().getStageNumber(),
                              k.getWorkerInputs().size(),
                              k.getStageDefinition().doesShuffle() ? "SHUFFLE" : "RETAIN",
                              k.getPhase(),
                              k.getWorkerInputs()
                               .stream()
                               .flatMap(rp -> StreamSupport.stream(rp.spliterator(), false))
                               .mapToInt(ReadablePartition::getPartitionNumber)
                               .distinct()
                               .count(),
                              k.hasResultPartitions() ? Iterators.size(k.getResultPartitions().iterator()) : -1
                          )
                     )
                     .collect(Collectors.joining("; "))
      );
    }
  }

  private interface TaskContactFn
  {
    void contactTask(TalariaTaskClient client, String taskId, int workerNumber);
  }
}
