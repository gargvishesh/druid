/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.imply.druid.talaria.counters.CounterSnapshots;
import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameChannelSequence;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByKeyReader;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.frame.processor.FrameProcessorExecutor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.indexing.ColumnMapping;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.indexing.DataSourceMSQDestination;
import io.imply.druid.talaria.indexing.InputChannelFactory;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.MSQControllerTask;
import io.imply.druid.talaria.indexing.SegmentGeneratorFrameProcessorFactory;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import io.imply.druid.talaria.indexing.TalariaWorkerTaskLauncher;
import io.imply.druid.talaria.indexing.TaskReportMSQDestination;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.CannotParseExternalDataFault;
import io.imply.druid.talaria.indexing.error.FaultsExceededChecker;
import io.imply.druid.talaria.indexing.error.InsertCannotAllocateSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertCannotBeEmptyFault;
import io.imply.druid.talaria.indexing.error.InsertCannotOrderByDescendingFault;
import io.imply.druid.talaria.indexing.error.InsertCannotReplaceExistingSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertLockPreemptedFault;
import io.imply.druid.talaria.indexing.error.InsertTimeOutOfBoundsFault;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.indexing.error.QueryNotSupportedFault;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TalariaWarnings;
import io.imply.druid.talaria.indexing.error.TooManyPartitionsFault;
import io.imply.druid.talaria.indexing.error.TooManyWarningsFault;
import io.imply.druid.talaria.indexing.error.WorkerFailedFault;
import io.imply.druid.talaria.indexing.report.TalariaResultsReport;
import io.imply.druid.talaria.indexing.report.TalariaStagesReport;
import io.imply.druid.talaria.indexing.report.TalariaStatusReport;
import io.imply.druid.talaria.indexing.report.TalariaTaskReport;
import io.imply.druid.talaria.indexing.report.TalariaTaskReportPayload;
import io.imply.druid.talaria.input.ExternalInputSpec;
import io.imply.druid.talaria.input.ExternalInputSpecSlicer;
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.InputSpecSlicer;
import io.imply.druid.talaria.input.InputSpecSlicerFactory;
import io.imply.druid.talaria.input.InputSpecs;
import io.imply.druid.talaria.input.MapInputSpecSlicer;
import io.imply.druid.talaria.input.StageInputSlice;
import io.imply.druid.talaria.input.StageInputSpec;
import io.imply.druid.talaria.input.StageInputSpecSlicer;
import io.imply.druid.talaria.input.TableInputSpec;
import io.imply.druid.talaria.input.TableInputSpecSlicer;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.QueryDefinitionBuilder;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ShuffleSpecFactories;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;
import io.imply.druid.talaria.kernel.TargetSizeShuffleSpec;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.kernel.controller.ControllerQueryKernel;
import io.imply.druid.talaria.kernel.controller.ControllerStagePhase;
import io.imply.druid.talaria.kernel.controller.WorkerInputs;
import io.imply.druid.talaria.querykit.DataSegmentTimelineView;
import io.imply.druid.talaria.querykit.MultiQueryKit;
import io.imply.druid.talaria.querykit.QueryKit;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.groupby.GroupByQueryKit;
import io.imply.druid.talaria.querykit.scan.ScanQueryKit;
import io.imply.druid.talaria.shuffle.DurableStorageInputChannelFactory;
import io.imply.druid.talaria.shuffle.WorkerInputChannelFactory;
import io.imply.druid.talaria.sql.TalariaQueryMaker;
import io.imply.druid.talaria.util.DimensionSchemaUtils;
import io.imply.druid.talaria.util.IntervalUtils;
import io.imply.druid.talaria.util.PassthroughAggregatorFactory;
import io.imply.druid.talaria.util.TalariaContext;
import io.imply.druid.talaria.util.TalariaFutureUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.QueryFeatureInspector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class LeaderImpl implements Leader
{
  private static final Logger log = new Logger(LeaderImpl.class);

  private final MSQControllerTask task;
  private final LeaderContext context;

  private final BlockingQueue<Consumer<ControllerQueryKernel>> kernelManipulationQueue =
      new ArrayBlockingQueue<>(Limits.MAX_KERNEL_MANIPULATION_QUEUE_SIZE);

  // For system error reporting. This is the very first error we got from a worker. (We only report that one.)
  private final AtomicReference<MSQErrorReport> workerErrorRef = new AtomicReference<>();

  // For system warning reporting
  private final ConcurrentLinkedQueue<MSQErrorReport> workerWarnings = new ConcurrentLinkedQueue<>();

  // For live reports.
  private final AtomicReference<QueryDefinition> queryDefRef = new AtomicReference<>();

  // For live reports. Last reported CounterSnapshots per stage per worker
  private final CounterSnapshotsTree taskCountersForLiveReports = new CounterSnapshotsTree();

  // For live reports. stage number -> stage phase
  private final ConcurrentHashMap<Integer, ControllerStagePhase> stagePhasesForLiveReports = new ConcurrentHashMap<>();

  // For live reports. stage number -> runtime interval. Endpoint is eternity's end if the stage is still running.
  private final ConcurrentHashMap<Integer, Interval> stageRuntimesForLiveReports = new ConcurrentHashMap<>();

  // For live reports. stage number -> worker count. Only set for stages that have started.
  private final ConcurrentHashMap<Integer, Integer> stageWorkerCountsForLiveReports = new ConcurrentHashMap<>();

  // For live reports. stage number -> partition count. Only set for stages that have started.
  private final ConcurrentHashMap<Integer, Integer> stagePartitionCountsForLiveReports = new ConcurrentHashMap<>();

  // For live reports. The time at which the query started
  private volatile DateTime queryStartTime = null;

  private volatile DruidNode selfDruidNode;
  private volatile TalariaWorkerTaskLauncher workerTaskLauncher;
  private volatile WorkerClient netClient;

  private volatile FaultsExceededChecker faultsExceededChecker = null;

  public LeaderImpl(
      final MSQControllerTask task,
      final LeaderContext context
  )
  {
    this.task = task;
    this.context = context;
  }

  @Override
  public String id()
  {
    return task.getId();
  }

  @Override
  public MSQControllerTask task()
  {
    return task;
  }

  @Override
  public TaskStatus run() throws Exception
  {
    final Closer closer = Closer.create();

    try {
      return runTask(closer);
    }
    catch (Throwable e) {
      try {
        closer.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      // We really don't expect this to error out. runTask should handle everything nicely. If it doesn't, something
      // strange happened, so log it.
      log.warn(e, "Encountered unhandled controller exception.");
      return TaskStatus.failure(id(), e.toString());
    }
    finally {
      closer.close();
    }
  }

  @Override
  public void stopGracefully()
  {
    final QueryDefinition queryDef = queryDefRef.get();

    // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
    log.info("Query [%s] canceled.", queryDef != null ? queryDef.getQueryId() : "<no id yet>");

    addToKernelManipulationQueue(
        kernel -> {
          throw new TalariaException(CanceledFault.INSTANCE);
        }
    );
  }

  public TaskStatus runTask(final Closer closer)
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<?> workerTaskRunnerFuture = null;
    CounterSnapshotsTree countersSnapshot = null;
    Yielder<Object[]> resultsYielder = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final MSQErrorReport errorForReport;

    try {
      this.queryStartTime = DateTimes.nowUtc();
      queryDef = initializeQueryDefAndState(closer);

      final InputSpecSlicerFactory inputSpecSlicerFactory = makeInputSpecSlicerFactory(makeDataSegmentTimelineView());
      final Pair<ControllerQueryKernel, ListenableFuture<?>> queryRunResult =
          runQueryUntilDone(queryDef, inputSpecSlicerFactory, closer);

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      resultsYielder = getFinalResultsYielder(queryDef, queryKernel);
      publishSegmentsIfNeeded(queryDef, queryKernel);
    }
    catch (Throwable e) {
      exceptionEncountered = e;
    }

    // Fetch final counters in separate try, in case runQueryUntilDone threw an exception.
    try {
      countersSnapshot = getFinalCountersSnapshot(queryKernel);
    }
    catch (Throwable e) {
      if (exceptionEncountered != null) {
        exceptionEncountered.addSuppressed(e);
      } else {
        exceptionEncountered = e;
      }
    }

    if (queryKernel != null && queryKernel.isSuccess() && exceptionEncountered == null) {
      taskStateForReport = TaskState.SUCCESS;
      errorForReport = null;
    } else {
      // Query failure. Generate an error report and log the error(s) we encountered.
      final String selfHost = TalariaTasks.getHostFromSelfNode(selfDruidNode);
      final MSQErrorReport controllerError =
          exceptionEncountered != null
          ? MSQErrorReport.fromException(id(), selfHost, null, exceptionEncountered)
          : null;
      final MSQErrorReport workerError = workerErrorRef.get();

      taskStateForReport = TaskState.FAILED;
      errorForReport = TalariaTasks.makeErrorReport(id(), selfHost, controllerError, workerError);

      // Log the errors we encountered.
      if (controllerError != null) {
        log.warn("Controller: %s", TalariaTasks.errorReportToLogMessage(controllerError));
      }

      if (workerError != null) {
        log.warn("Worker: %s", TalariaTasks.errorReportToLogMessage(workerError));
      }
    }

    try {
      // Write report even if something went wrong.
      final TalariaStagesReport stagesReport;
      final TalariaResultsReport resultsReport;

      if (queryDef != null) {
        final Map<Integer, ControllerStagePhase> stagePhaseMap;

        if (queryKernel != null) {
          // Once the query finishes, cleanup would have happened for all the stages that were successful
          // Therefore we mark it as done to make the reports prettier and more accurate
          queryKernel.markSuccessfulTerminalStagesAsFinished();
          stagePhaseMap = queryKernel.getActiveStages()
                                     .stream()
                                     .collect(
                                         Collectors.toMap(StageId::getStageNumber, queryKernel::getStagePhase)
                                     );
        } else {
          stagePhaseMap = Collections.emptyMap();
        }

        stagesReport = makeStageReport(
            queryDef,
            stagePhaseMap,
            stageRuntimesForLiveReports,
            stageWorkerCountsForLiveReports,
            stagePartitionCountsForLiveReports
        );
      } else {
        stagesReport = null;
      }

      if (resultsYielder != null) {
        resultsReport = makeResultsTaskReport(
            queryDef,
            resultsYielder,
            task.getQuerySpec().getColumnMappings(),
            task.getSqlTypeNames()
        );
      } else {
        resultsReport = null;
      }

      final TalariaTaskReportPayload taskReportPayload = new TalariaTaskReportPayload(
          makeStatusReport(
              taskStateForReport,
              errorForReport,
              workerWarnings,
              queryStartTime,
              new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis()
          ),
          stagesReport,
          countersSnapshot,
          resultsReport
      );

      context.writeReports(
          id(),
          TaskReport.buildTaskReports(new TalariaTaskReport(id(), taskReportPayload))
      );
    }
    catch (Throwable e) {
      log.warn(e, "Error encountered while writing task report. Skipping.");
    }

    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage the tasks to exit successfully.
      postFinishToAllTasks();
      workerTaskLauncher.stop(false);
    } else {
      // If not successful, cancel running tasks.
      // It can be possible that the query fails in initializeQueryDefAndState. In such a case, workerTaskLauncher can be null.
      if (workerTaskLauncher != null) {
        workerTaskLauncher.stop(true);
      }
    }

    // Wait for worker tasks to exit. Ignore their return status. At this point, we've done everything we need to do,
    // so we don't care about the task exit status.
    if (workerTaskRunnerFuture != null) {
      try {
        workerTaskRunnerFuture.get();
      }
      catch (Exception ignored) {
        // Suppress.
      }
    }

    cleanUpDurableStorageIfNeeded();

    if (taskStateForReport == TaskState.SUCCESS) {
      return TaskStatus.success(id());
    } else {
      // errorForReport is nonnull when taskStateForReport != SUCCESS. Use that message.
      return TaskStatus.failure(id(), errorForReport.getFault().getCodeWithMessage());
    }
  }

  private QueryDefinition initializeQueryDefAndState(final Closer closer)
  {
    this.selfDruidNode = context.selfNode();
    context.registerLeader(this, closer);

    this.netClient = new ExceptionWrappingWorkerClient(context.taskClientFor(this));
    closer.register(netClient::close);

    final boolean isDurableStorageEnabled =
        TalariaContext.isDurableStorageEnabled(task.getQuerySpec().getQuery().getContext());

    final QueryDefinition queryDef = makeQueryDefinition(
        id(),
        makeQueryControllerToolKit(),
        task.getQuerySpec()
    );

    QueryValidator.validateQueryDef(queryDef);
    queryDefRef.set(queryDef);

    log.debug("Query [%s] durable storage mode is set to %s.", queryDef.getQueryId(), isDurableStorageEnabled);

    this.workerTaskLauncher = new TalariaWorkerTaskLauncher(
        id(),
        task.getDataSource(),
        context,
        isDurableStorageEnabled,

        // 10 minutes +- 2 minutes jitter
        TimeUnit.SECONDS.toMillis(600 + ThreadLocalRandom.current().nextInt(-4, 5) * 30L)
    );

    long maxParseExceptions = -1;

    if (task.getSqlQueryContext() != null) {
      maxParseExceptions = Optional.ofNullable(
                                       task.getSqlQueryContext().get(TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED))
                                   .map(DimensionHandlerUtils::convertObjectToLong)
                                   .orElse(TalariaWarnings.DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED);
    }

    this.faultsExceededChecker = new FaultsExceededChecker(
        ImmutableMap.of(CannotParseExternalDataFault.CODE, maxParseExceptions)
    );

    return queryDef;
  }

  private Pair<ControllerQueryKernel, ListenableFuture<?>> runQueryUntilDone(
      final QueryDefinition queryDef,
      final InputSpecSlicerFactory inputSpecSlicerFactory,
      final Closer closer
  ) throws Exception
  {
    // Start tasks.
    log.debug("Query [%s] starting tasks.", queryDef.getQueryId());

    final ListenableFuture<?> workerTaskLauncherFuture = workerTaskLauncher.start();
    closer.register(() -> workerTaskLauncher.stop(true));

    workerTaskLauncherFuture.addListener(
        () ->
            addToKernelManipulationQueue(queryKernel -> {
              // Throw an exception in the main loop, if anything went wrong.
              TalariaFutureUtils.getUncheckedImmediately(workerTaskLauncherFuture);
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
      final List<StageId> newStageIds = queryKernel.createAndGetNewStageIds(
          inputSpecSlicerFactory,
          task.getQuerySpec().getAssignmentStrategy()
      );

      for (final StageId stageId : newStageIds) {
        queryKernel.startStage(stageId);

        // Allocate segments, if this is the final stage of an ingestion.
        if (MSQControllerTask.isIngestion(task.getQuerySpec())
            && stageId.getStageNumber() == queryDef.getFinalStageDefinition().getStageNumber()) {
          // We need to find the shuffle details (like partition ranges) to generate segments. Generally this is
          // going to correspond to the stage immediately prior to the final segment-generator stage.
          int shuffleStageNumber = Iterables.getOnlyElement(queryDef.getFinalStageDefinition().getInputStageNumbers());

          // The following logic assumes that output of all the stages without a shuffle retain the partition boundaries
          // of the input to that stage. This may not always be the case. For example GROUP BY queries without an ORDER BY
          // clause. This works for QueryKit generated queries uptil now, but it should be reworked as it might not
          // always be the case
          while (!queryDef.getStageDefinition(shuffleStageNumber).doesShuffle()) {
            shuffleStageNumber =
                Iterables.getOnlyElement(queryDef.getStageDefinition(shuffleStageNumber).getInputStageNumbers());
          }

          final StageId shuffleStageId = new StageId(queryDef.getQueryId(), shuffleStageNumber);
          final boolean isTimeBucketed = isTimeBucketedIngestion(task.getQuerySpec());
          final ClusterByPartitions partitionBoundaries =
              queryKernel.getResultPartitionBoundariesForStage(shuffleStageId);

          // We require some data to be inserted in case it is partitioned by anything other than all and we are
          // inserting everything into a single bucket. This can be handled more gracefully instead of throwing an exception
          // Note: This can also be the case when we have limit queries but validation in Broker SQL layer prevents such
          // queries
          if (isTimeBucketed && partitionBoundaries.equals(ClusterByPartitions.oneUniversalPartition())) {
            throw new TalariaException(new InsertCannotBeEmptyFault(task.getDataSource()));
          } else {
            log.info("Query [%s] generating %d segments.", queryDef.getQueryId(), partitionBoundaries.size());
          }

          final boolean mayHaveMultiValuedClusterByFields =
              !queryKernel.getStageDefinition(shuffleStageId).mustGatherResultKeyStatistics()
              || queryKernel.hasStageCollectorEncounteredAnyMultiValueField(shuffleStageId);

          segmentsToGenerate = generateSegmentIdsWithShardSpecs(
              (DataSourceMSQDestination) task.getQuerySpec().getDestination(),
              queryKernel.getStageDefinition(shuffleStageId).getSignature(),
              queryKernel.getStageDefinition(shuffleStageId).getShuffleSpec().get().getClusterBy(),
              partitionBoundaries,
              mayHaveMultiValuedClusterByFields
          );
        }

        final int workerCount = queryKernel.getWorkerInputsForStage(stageId).workerCount();
        log.info(
            "Query [%s] starting %d workers for stage %d.",
            stageId.getQueryId(),
            workerCount,
            stageId.getStageNumber()
        );

        workerTaskLauncher.launchTasksIfNeeded(workerCount);
        stageRuntimesForLiveReports.put(stageId.getStageNumber(), new Interval(DateTimes.nowUtc(), DateTimes.MAX));
        startWorkForStage(queryDef, queryKernel, stageId.getStageNumber(), segmentsToGenerate);
      }

      // Send partition boundaries to tasks, if the time is right.
      logKernelStatus(queryDef.getQueryId(), queryKernel);
      for (final StageId stageId : queryKernel.getActiveStages()) {

        if (queryKernel.getStageDefinition(stageId).mustGatherResultKeyStatistics()
            && queryKernel.doesStageHaveResultPartitions(stageId)
            && stageResultPartitionBoundariesSent.add(stageId)) {
          if (log.isDebugEnabled()) {
            final ClusterByPartitions partitions = queryKernel.getResultPartitionBoundariesForStage(stageId);
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
              queryKernel.getResultPartitionBoundariesForStage(stageId),
              queryKernel.getWorkerInputsForStage(stageId).workers()
          );
        }
      }

      logKernelStatus(queryDef.getQueryId(), queryKernel);

      // Live reports: update stage phases, worker counts, partition counts.
      for (StageId stageId : queryKernel.getActiveStages()) {
        final int stageNumber = stageId.getStageNumber();
        stagePhasesForLiveReports.put(stageNumber, queryKernel.getStagePhase(stageId));

        if (queryKernel.doesStageHaveResultPartitions(stageId)) {
          stagePartitionCountsForLiveReports.computeIfAbsent(
              stageNumber,
              k -> Iterators.size(queryKernel.getResultPartitionsForStage(stageId).iterator())
          );
        }

        stageWorkerCountsForLiveReports.putIfAbsent(
            stageNumber,
            queryKernel.getWorkerInputsForStage(stageId).workerCount()
        );
      }

      // Live reports: update stage end times for any stages that just ended.
      for (StageId stageId : queryKernel.getActiveStages()) {
        if (ControllerStagePhase.isSuccessfulTerminalPhase(queryKernel.getStagePhase(stageId))) {
          stageRuntimesForLiveReports.compute(
              queryKernel.getStageDefinition(stageId).getStageNumber(),
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

      // Notify the workers to clean up the stages which can be marked as finished.
      for (final StageId stageId : queryKernel.getEffectivelyFinishedStageIds()) {
        log.info("Query [%s] issuing cleanup order for stage %d.", queryDef.getQueryId(), stageId.getStageNumber());
        contactWorkersForStage(
            (netClient, taskId, workerNumber) -> netClient.postCleanupStage(taskId, stageId),
            queryKernel.getWorkerInputsForStage(stageId).workers()
        );
        queryKernel.finishStage(stageId, true);
      }

      if (!queryKernel.isDone()) {
        // Wait for next command, run it, then look through the query tracker again.
        kernelManipulationQueue.take().accept(queryKernel);
      }
    }

    if (!queryKernel.isSuccess()) {
      // Look for a known failure reason and throw a meaningful exception.
      for (final StageId stageId : queryKernel.getActiveStages()) {
        if (queryKernel.getStagePhase(stageId) == ControllerStagePhase.FAILED) {
          switch (queryKernel.getFailureReasonForStage(stageId)) {
            case TOO_MANY_PARTITIONS:
              throw new TalariaException(
                  new TooManyPartitionsFault(queryKernel.getStageDefinition(stageId).getMaxPartitionCount())
              );

            default:
              // Fall through.
          }
        }
      }
    }

    return Pair.of(queryKernel, workerTaskLauncherFuture);
  }

  /**
   * Provide a {@link ClusterByStatisticsSnapshot} for shuffling stages.
   */
  @Override
  public void updateStatus(int stageNumber, int workerNumber, Object keyStatisticsObject)
  {
    addToKernelManipulationQueue(
        queryKernel -> {
          final StageId stageId = queryKernel.getStageId(stageNumber);
          if (stageId == null) {
            throw new ISE("Unable to find stage with id [%s].", stageId);
          }

          // We need a specially-decorated ObjectMapper to deserialize key statistics.
          final StageDefinition stageDef = queryKernel.getStageDefinition(stageId);
          final ObjectMapper mapper = TalariaTasks.decorateObjectMapperForKeyCollectorSnapshot(
              context.jsonMapper(),
              stageDef.getShuffleSpec().get().getClusterBy(),
              stageDef.getShuffleSpec().get().doesAggregateByClusterKey()
          );

          final ClusterByStatisticsSnapshot keyStatistics;
          try {
            keyStatistics = mapper.convertValue(keyStatisticsObject, ClusterByStatisticsSnapshot.class);
          }
          catch (IllegalArgumentException e) {
            throw new IAE(
                e,
                "Unable to deserialize the key statistic for stage [%s] received from the worker [%d]",
                stageId,
                workerNumber
            );
          }

          queryKernel.addResultKeyStatisticsForStageAndWorker(stageId, workerNumber, keyStatistics);
        }
    );
  }

  @Override
  public void workerError(MSQErrorReport errorReport)
  {
    if (!workerTaskLauncher.isTaskCanceledByLeader(errorReport.getTaskId())) {
      workerErrorRef.compareAndSet(null, errorReport);
    }
  }

  /**
   * This method intakes all the warnings that are generated by the worker. It is the responsibility of the
   * worker node to ensure that it doesn't spam the leader with unneseccary warning stack traces. Currently, that
   * limiting is implemented in {@link io.imply.druid.talaria.indexing.error.TalariaWarningReportLimiterPublisher}
   */
  @Override
  public void workerWarning(List<MSQErrorReport> errorReports)
  {
    // This check is just to safeguard that leader doesn't run out of memory. The actual limiting should be done
    // on the worker's side as well. Also, it uses double lock checking
    long numReportsToAddCheck = Math.min(
        errorReports.size(),
        Limits.MAX_WORKERS * Limits.MAX_VERBOSE_WARNINGS - workerWarnings.size()
    );
    if (numReportsToAddCheck > 0) {
      synchronized (workerWarnings) {
        long numReportsToAdd = Math.min(
            errorReports.size(),
            Limits.MAX_WORKERS * Limits.MAX_VERBOSE_WARNINGS - workerWarnings.size()
        );
        for (int i = 0; i < numReportsToAdd; ++i) {
          workerWarnings.add(errorReports.get(i));
        }
      }
    }
  }

  @Override
  public void workerFailed(final WorkerFailedFault fault)
  {
    workerError(MSQErrorReport.fromFault(id(), selfDruidNode.getHost(), null, fault));
  }

  /**
   * Periodic update of {@link CounterSnapshots} from subtasks.
   */
  @Override
  public void updateCounters(CounterSnapshotsTree snapshotsTree)
  {
    taskCountersForLiveReports.putAll(snapshotsTree);
    Optional<Pair<String, Long>> warningsExceeded =
        faultsExceededChecker.addFaultsAndCheckIfExceeded(taskCountersForLiveReports);

    if (warningsExceeded.isPresent()) {
      String errorCode = warningsExceeded.get().lhs;
      Long limit = warningsExceeded.get().rhs;
      workerError(MSQErrorReport.fromFault(
          id(),
          selfDruidNode.getHost(),
          null,
          new TooManyWarningsFault(limit.intValue(), errorCode)
      ));
      addToKernelManipulationQueue(
          queryKernel ->
              queryKernel.getActiveStages().forEach(queryKernel::failStage)
      );
    }
  }

  /**
   * Reports that results are ready for a subtask.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void resultsComplete(
      final String queryId,
      final int stageNumber,
      final int workerNumber,
      Object resultObject
  )
  {
    addToKernelManipulationQueue(
        queryKernel -> {
          final StageId stageId = new StageId(queryId, stageNumber);
          if (stageId == null) {
            throw new ISE("Unable to find stage with id [%s]", stageId);
          }
          final Object convertedResultObject;
          try {
            convertedResultObject = context.jsonMapper().convertValue(
                resultObject,
                queryKernel.getStageDefinition(stageId).getProcessorFactory().getAccumulatedResultTypeReference()
            );
          }
          catch (IllegalArgumentException e) {
            throw new IAE(
                e,
                "Unable to deserialize the result object for stage [%s] received from the worker [%d]",
                stageId,
                workerNumber
            );
          }


          queryKernel.setResultsCompleteForStageAndWorker(stageId, workerNumber, convertedResultObject);
        }
    );
  }

  @Override
  @Nullable
  public Map<String, TaskReport> liveReports()
  {
    final QueryDefinition queryDef = queryDefRef.get();

    if (queryDef == null) {
      return null;
    }

    return TaskReport.buildTaskReports(
        new TalariaTaskReport(
            id(),
            new TalariaTaskReportPayload(
                makeStatusReport(
                    TaskState.RUNNING,
                    null,
                    workerWarnings,
                    queryStartTime,
                    queryStartTime == null ? -1L : new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis()
                ),
                makeStageReport(
                    queryDef,
                    stagePhasesForLiveReports,
                    stageRuntimesForLiveReports,
                    stageWorkerCountsForLiveReports,
                    stagePartitionCountsForLiveReports
                ),
                makeCountersSnapshotForLiveReports(),
                null
            )
        )
    );
  }

  /**
   * Returns the segments that will be generated by this job. Delegates to
   * {@link #generateSegmentIdsWithShardSpecsForAppend} or {@link #generateSegmentIdsWithShardSpecsForReplace} as
   * appropriate. This is a potentially expensive call, since it requires calling Overlord APIs.
   *
   * @throws TalariaException with {@link InsertCannotAllocateSegmentFault} if an allocation cannot be made
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecs(
      final DataSourceMSQDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean mayHaveMultiValuedClusterByFields
  ) throws IOException
  {
    if (destination.isReplaceTimeChunks()) {
      return generateSegmentIdsWithShardSpecsForReplace(
          destination,
          signature,
          clusterBy,
          partitionBoundaries,
          mayHaveMultiValuedClusterByFields
      );
    } else {
      final ClusterByKeyReader keyReader = clusterBy.keyReader(signature);
      return generateSegmentIdsWithShardSpecsForAppend(destination, partitionBoundaries, keyReader);
    }
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecsForAppend(
      final DataSourceMSQDestination destination,
      final ClusterByPartitions partitionBoundaries,
      final ClusterByKeyReader keyReader
  ) throws IOException
  {
    final Granularity segmentGranularity = destination.getSegmentGranularity();

    String previousSegmentId = null;

    final List<SegmentIdWithShardSpec> retVal = new ArrayList<>(partitionBoundaries.size());

    for (ClusterByPartition partitionBoundary : partitionBoundaries) {
      final DateTime timestamp = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
      final SegmentIdWithShardSpec allocation;
      try {
        allocation = context.taskActionClient().submit(
            new SegmentAllocateAction(
                task.getDataSource(),
                timestamp,
                // Same granularity for queryGranularity, segmentGranularity because we don't have insight here
                // into what queryGranularity "actually" is. (It depends on what time floor function was used.)
                segmentGranularity,
                segmentGranularity,
                id(),
                previousSegmentId,
                false,
                NumberedPartialShardSpec.instance(),
                LockGranularity.TIME_CHUNK,
                TaskLockType.SHARED
            )
        );
      }
      catch (ISE e) {
        if (isTaskLockPreemptedException(e)) {
          throw new TalariaException(e, InsertLockPreemptedFault.instance());
        } else {
          throw e;
        }
      }

      if (allocation == null) {
        throw new TalariaException(
            new InsertCannotAllocateSegmentFault(
                task.getDataSource(),
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
      final DataSourceMSQDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean mayHaveMultiValuedClusterByFields
  ) throws IOException
  {
    final ClusterByKeyReader keyReader = clusterBy.keyReader(signature);
    final SegmentIdWithShardSpec[] retVal = new SegmentIdWithShardSpec[partitionBoundaries.size()];
    final Granularity segmentGranularity = destination.getSegmentGranularity();
    final List<String> shardColumns;

    if (mayHaveMultiValuedClusterByFields) {
      // DimensionRangeShardSpec cannot handle multi-valued fields.
      shardColumns = Collections.emptyList();
    } else {
      shardColumns = computeShardColumns(signature, clusterBy, task.getQuerySpec().getColumnMappings());
    }

    // Group partition ranges by bucket (time chunk), so we can generate shardSpecs for each bucket independently.
    final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket = new HashMap<>();
    for (int i = 0; i < partitionBoundaries.ranges().size(); i++) {
      ClusterByPartition partitionBoundary = partitionBoundaries.ranges().get(i);
      final DateTime bucketDateTime = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
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

      final List<TaskLock> locks = context.taskActionClient().submit(new LockListAction());
      for (final TaskLock lock : locks) {
        if (lock.getInterval().contains(interval)) {
          version = lock.getVersion();
        }
      }

      if (version == null) {
        // Lock was revoked, probably, because we should have originally acquired it in isReady.
        throw new TalariaException(new InsertCannotAllocateSegmentFault(task.getDataSource(), interval));
      }

      for (int segmentNumber = 0; segmentNumber < ranges.size(); segmentNumber++) {
        final int partitionNumber = ranges.get(segmentNumber).lhs;
        final ShardSpec shardSpec;

        if (shardColumns.isEmpty()) {
          shardSpec = new NumberedShardSpec(segmentNumber, ranges.size());
        } else {
          final ClusterByPartition range = ranges.get(segmentNumber).rhs;
          final StringTuple start =
              segmentNumber == 0 ? null : makeStringTuple(clusterBy, keyReader, range.getStart());
          final StringTuple end =
              segmentNumber == ranges.size() - 1 ? null : makeStringTuple(clusterBy, keyReader, range.getEnd());

          shardSpec = new DimensionRangeShardSpec(shardColumns, start, end, segmentNumber, ranges.size());
        }

        retVal[partitionNumber] = new SegmentIdWithShardSpec(task.getDataSource(), interval, version, shardSpec);
      }
    }

    return Arrays.asList(retVal);
  }

  /**
   * Returns a complete list of task ids, ordered by worker number. The Nth task has worker number N.
   * <p>
   * If the currently-running set of tasks is incomplete, returns an absent Optional.
   */
  @Override
  public List<String> getTaskIds()
  {
    if (workerTaskLauncher == null) {
      return Collections.emptyList();
    }

    return workerTaskLauncher.getTaskList();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Nullable
  private Int2ObjectMap<Object> makeWorkerFactoryInfosForStage(
      final QueryDefinition queryDef,
      final int stageNumber,
      final WorkerInputs workerInputs,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    if (MSQControllerTask.isIngestion(task.getQuerySpec()) &&
        stageNumber == queryDef.getFinalStageDefinition().getStageNumber()) {
      // noinspection unchecked,rawtypes
      return (Int2ObjectMap) makeSegmentGeneratorWorkerFactoryInfos(workerInputs, segmentsToGenerate);
    } else {
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  private QueryKit makeQueryControllerToolKit()
  {
    final Map<Class<? extends Query>, QueryKit> kitMap =
        ImmutableMap.<Class<? extends Query>, QueryKit>builder()
                    .put(ScanQuery.class, new ScanQueryKit(context.jsonMapper()))
                    .put(GroupByQuery.class, new GroupByQueryKit())
                    .build();

    return new MultiQueryKit(kitMap);
  }

  private DataSegmentTimelineView makeDataSegmentTimelineView()
  {
    return (dataSource, intervals) -> {
      final Collection<DataSegment> dataSegments =
          context.coordinatorClient().fetchUsedSegmentsInDataSourceForIntervals(dataSource, intervals);

      if (dataSegments.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(VersionedIntervalTimeline.forSegments(dataSegments));
      }
    };
  }

  private Int2ObjectMap<List<SegmentIdWithShardSpec>> makeSegmentGeneratorWorkerFactoryInfos(
      final WorkerInputs workerInputs,
      final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final Int2ObjectMap<List<SegmentIdWithShardSpec>> retVal = new Int2ObjectAVLTreeMap<>();

    for (final int workerNumber : workerInputs.workers()) {
      // SegmentGenerator stage has a single input from another stage.
      final StageInputSlice stageInputSlice =
          (StageInputSlice) Iterables.getOnlyElement(workerInputs.inputsForWorker(workerNumber));

      final List<SegmentIdWithShardSpec> workerSegments = new ArrayList<>();
      retVal.put(workerNumber, workerSegments);

      for (final ReadablePartition partition : stageInputSlice.getPartitions()) {
        workerSegments.add(segmentsToGenerate.get(partition.getPartitionNumber()));
      }
    }

    return retVal;
  }

  private void contactWorkersForStage(final TaskContactFn contactFn, final IntSet workers)
  {
    final List<String> taskIds = getTaskIds();
    final List<ListenableFuture<Void>> taskFutures = new ArrayList<>(workers.size());

    for (int workerNumber : workers) {
      final String taskId = taskIds.get(workerNumber);
      taskFutures.add(contactFn.contactTask(netClient, taskId, workerNumber));
    }

    FutureUtils.getUnchecked(TalariaFutureUtils.allAsList(taskFutures, true), true);
  }

  private void startWorkForStage(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel,
      final int stageNumber,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final Int2ObjectMap<Object> extraInfos = makeWorkerFactoryInfosForStage(
        queryDef,
        stageNumber,
        queryKernel.getWorkerInputsForStage(queryKernel.getStageId(stageNumber)),
        segmentsToGenerate
    );

    final Int2ObjectMap<WorkOrder> workOrders = queryKernel.createWorkOrders(stageNumber, extraInfos);

    contactWorkersForStage(
        (netClient, taskId, workerNumber) -> netClient.postWorkOrder(taskId, workOrders.get(workerNumber)),
        workOrders.keySet()
    );
  }

  private void postResultPartitionBoundariesForStage(
      final QueryDefinition queryDef,
      final int stageNumber,
      final ClusterByPartitions resultPartitionBoundaries,
      final IntSet workers
  )
  {
    contactWorkersForStage(
        (netClient, taskId, workerNumber) ->
            netClient.postResultPartitionBoundaries(
                taskId,
                new StageId(queryDef.getQueryId(), stageNumber),
                resultPartitionBoundaries
            ),
        workers
    );
  }

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceMSQDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   * <p>
   * If any existing segments cannot be dropped because their intervals are not wholly contained within the
   * replacement parameter, throws a {@link TalariaException} with {@link InsertCannotReplaceExistingSegmentFault}.
   */
  private void publishAllSegments(final Set<DataSegment> segments) throws IOException
  {
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final Set<DataSegment> segmentsToDrop;

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (intervalsToDrop.isEmpty()) {
        segmentsToDrop = null;
      } else {
        segmentsToDrop =
            ImmutableSet.copyOf(
                context.taskActionClient().submit(
                    new RetrieveUsedSegmentsAction(
                        task.getDataSource(),
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
        for (final Interval interval : intervalsToDrop) {
          context.taskActionClient()
                 .submit(new MarkSegmentsAsUnusedAction(task.getDataSource(), interval));
        }
      } else {
        try {
          context.taskActionClient()
                 .submit(SegmentTransactionalInsertAction.overwriteAction(null, segmentsToDrop, segments));
        }
        catch (Exception e) {
          if (isTaskLockPreemptedException(e)) {
            throw new TalariaException(e, InsertLockPreemptedFault.instance());
          } else {
            throw e;
          }
        }
      }
    } else if (!segments.isEmpty()) {
      // Append mode.
      try {
        context.taskActionClient().submit(new SegmentInsertAction(segments));
      }
      catch (Exception e) {
        if (isTaskLockPreemptedException(e)) {
          throw new TalariaException(e, InsertLockPreemptedFault.instance());
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * When doing an ingestion with {@link DataSourceMSQDestination#isReplaceTimeChunks()}, finds intervals
   * containing data that should be dropped.
   */
  private List<Interval> findIntervalsToDrop(final Set<DataSegment> publishedSegments)
  {
    // Safe to cast because publishAllSegments is only called for dataSource destinations.
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  private CounterSnapshotsTree getCountersFromAllTasks()
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();
    final List<String> taskList = workerTaskLauncher.getTaskList();

    final List<ListenableFuture<CounterSnapshotsTree>> futures = new ArrayList<>();

    for (String taskId : taskList) {
      futures.add(netClient.getCounters(taskId));
    }

    final List<CounterSnapshotsTree> snapshotsTrees =
        FutureUtils.getUnchecked(TalariaFutureUtils.allAsList(futures, true), true);

    for (CounterSnapshotsTree snapshotsTree : snapshotsTrees) {
      retVal.putAll(snapshotsTree);
    }

    return retVal;
  }

  private void postFinishToAllTasks()
  {
    final List<String> taskList = workerTaskLauncher.getTaskList();

    final List<ListenableFuture<Void>> futures = new ArrayList<>();

    for (String taskId : taskList) {
      futures.add(netClient.postFinish(taskId));
    }

    FutureUtils.getUnchecked(TalariaFutureUtils.allAsList(futures, true), true);
  }

  private CounterSnapshotsTree makeCountersSnapshotForLiveReports()
  {
    // taskCountersForLiveReports is mutable: Copy so we get a point-in-time snapshot.
    return CounterSnapshotsTree.fromMap(taskCountersForLiveReports.copyMap());
  }

  private CounterSnapshotsTree getFinalCountersSnapshot(@Nullable final ControllerQueryKernel queryKernel)
  {
    if (queryKernel != null && queryKernel.isSuccess()) {
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
    if (queryKernel.isSuccess() && isInlineResults(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      final List<String> taskIds = getTaskIds();
      final Closer closer = Closer.create();

      final ListeningExecutorService resultReaderExec =
          MoreExecutors.listeningDecorator(Execs.singleThreaded("result-reader-%d"));
      closer.register(resultReaderExec::shutdownNow);

      final InputChannelFactory inputChannelFactory;

      if (TalariaContext.isDurableStorageEnabled(task.getQuerySpec().getQuery().getContext())) {
        inputChannelFactory = DurableStorageInputChannelFactory.createStandardImplementation(
            id(),
            () -> taskIds,
            TalariaTasks.makeStorageConnector(context.injector()),
            closer
        );
      } else {
        inputChannelFactory = new WorkerInputChannelFactory(netClient, () -> taskIds);
      }

      final InputChannels inputChannels = InputChannels.create(
          queryDef,
          queryKernel.getResultPartitionsForStage(finalStageId),
          inputChannelFactory,
          () -> ArenaMemoryAllocator.createOnHeap(5_000_000),
          new FrameProcessorExecutor(resultReaderExec),
          null
      );

      return Yielders.each(
          Sequences.concat(
              StreamSupport.stream(queryKernel.getResultPartitionsForStage(finalStageId).spliterator(), false)
                           .map(
                               readablePartition -> {
                                 try {
                                   return new FrameChannelSequence(
                                       inputChannels.openChannel(
                                           new StagePartition(
                                               queryKernel.getStageDefinition(finalStageId).getId(),
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
                    queryKernel.getStageDefinition(finalStageId).getFrameReader()
                );

                final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
                final ColumnMappings columnMappings = task.getQuerySpec().getColumnMappings();
                @SuppressWarnings("rawtypes")
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
    if (queryKernel.isSuccess() && MSQControllerTask.isIngestion(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());

      //noinspection unchecked
      @SuppressWarnings("unchecked")
      final Set<DataSegment> segments = (Set<DataSegment>) queryKernel.getResultObjectForStage(finalStageId);
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments);
    }
  }

  private void cleanUpDurableStorageIfNeeded()
  {
    if (TalariaContext.isDurableStorageEnabled(task.getQuerySpec().getQuery().getContext())) {
      final String leaderDirName = StringUtils.format("controller_%s", task.getId());
      try {
        // Delete all temporary files as a failsafe
        TalariaTasks.makeStorageConnector(context.injector()).deleteRecursively(leaderDirName);
      }
      catch (Exception e) {
        // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
        log.warn(e, "Error while cleaning up temporary files at path %s", leaderDirName);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final String queryId,
      @SuppressWarnings("rawtypes") final QueryKit toolKit,
      final TalariaQuerySpec querySpec
  )
  {
    final ParallelIndexTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ShuffleSpecFactory shuffleSpecFactory;

    if (MSQControllerTask.isIngestion(querySpec)) {
      shuffleSpecFactory = (clusterBy, aggregate) ->
          new TargetSizeShuffleSpec(
              clusterBy,
              tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
              aggregate
          );
    } else if (querySpec.getDestination() instanceof TaskReportMSQDestination) {
      shuffleSpecFactory = ShuffleSpecFactories.singlePartition();
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }

    final QueryDefinition queryDef;

    try {
      //noinspection unchecked
      queryDef = toolKit.makeQueryDefinition(
          queryId,
          querySpec.getQuery(),
          toolKit,
          shuffleSpecFactory,
          tuningConfig.getMaxNumConcurrentSubTasks(),
          0
      );
    }
    catch (TalariaException e) {
      // If the toolkit throws a TalariaFault, donot wrap it in a more generic QueryNotSupportedFault
      throw e;
    }
    catch (Exception e) {
      throw new TalariaException(e, QueryNotSupportedFault.INSTANCE);
    }

    if (MSQControllerTask.isIngestion(querySpec)) {
      final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
      final ClusterBy queryClusterBy = queryDef.getFinalStageDefinition().getClusterBy();
      final ColumnMappings columnMappings = querySpec.getColumnMappings();

      // Find the stage that provides shuffled input to the final segment-generation stage.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();

      while (!finalShuffleStageDef.doesShuffle()
             && InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()).size() == 1) {
        finalShuffleStageDef = queryDef.getStageDefinition(
            Iterables.getOnlyElement(InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()))
        );
      }

      if (!finalShuffleStageDef.doesShuffle()) {
        finalShuffleStageDef = null;
      }

      // Add all query stages.
      // Set shuffleCheckHasMultipleValues on the stage that serves as input to the final segment-generation stage.
      final QueryDefinitionBuilder builder = QueryDefinition.builder();

      for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
        if (stageDef.equals(finalShuffleStageDef)) {
          builder.add(StageDefinition.builder(stageDef).shuffleCheckHasMultipleValues(true));
        } else {
          builder.add(StageDefinition.builder(stageDef));
        }
      }

      // Then, add a segment-generation stage.
      final DataSchema dataSchema = generateDataSchema(querySpec, querySignature, queryClusterBy, columnMappings);
      builder.add(
          StageDefinition.builder(queryDef.getNextStageNumber())
                         .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                         .maxWorkerCount(tuningConfig.getMaxNumConcurrentSubTasks())
                         .processorFactory(
                             new SegmentGeneratorFrameProcessorFactory(
                                 dataSchema,
                                 columnMappings,
                                 tuningConfig
                             )
                         )
      );

      return builder.build();
    } else if (querySpec.getDestination() instanceof TaskReportMSQDestination) {
      return queryDef;
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }

  private static DataSchema generateDataSchema(
      TalariaQuerySpec querySpec,
      RowSignature querySignature,
      ClusterBy queryClusterBy,
      ColumnMappings columnMappings
  )
  {
    final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
    final boolean isRollupQ = isRollupQuery(querySpec.getQuery());

    final Pair<List<DimensionSchema>, List<AggregatorFactory>> dimensionsAndAggregators =
        makeDimensionsAndAggregatorsForIngestion(
            querySignature,
            queryClusterBy,
            destination.getSegmentSortOrder(),
            columnMappings,
            isRollupQ,
            querySpec.getQuery()
        );

    return new DataSchema(
        destination.getDataSource(),
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(dimensionsAndAggregators.lhs),
        dimensionsAndAggregators.rhs.toArray(new AggregatorFactory[0]),
        makeGranularitySpecForIngestion(isRollupQ, querySpec.getQuery()),
        new TransformSpec(null, Collections.emptyList())
    );
  }

  private static GranularitySpec makeGranularitySpecForIngestion(boolean isRollupQ, Query<?> query)
  {
    if (!isRollupQ) {
      return new ArbitraryGranularitySpec(Granularities.NONE, false, Intervals.ONLY_ETERNITY);
    } else {
      final String queryGranularity = query.getContextValue(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, "");

      if (checkIfTimeColumnsAreEqual((GroupByQuery) query) && !queryGranularity.isEmpty()) {
        return new ArbitraryGranularitySpec(
            Granularity.fromString(queryGranularity),
            true,
            Intervals.ONLY_ETERNITY
        );
      }
      return new ArbitraryGranularitySpec(Granularities.NONE, true, Intervals.ONLY_ETERNITY);
    }
  }


  /**
   * Checks if the time columns present in the groupByQuery context are same. One is set by
   * {@link DruidQuery#toGroupByQuery(QueryFeatureInspector)} and the other is set by
   * {@link TalariaQueryMaker#runQuery(DruidQuery)}
   *
   * @param groupByQuery
   *
   * @return true if both groupByQuery context values are present and equal else returns false.
   */
  private static boolean checkIfTimeColumnsAreEqual(GroupByQuery groupByQuery)
  {
    final String talariaTimeColumn = groupByQuery.getContextValue(QueryKitUtils.CTX_TIME_COLUMN_NAME, "");
    if (talariaTimeColumn.isEmpty()) {
      return false;
    }
    return talariaTimeColumn.equals(groupByQuery.getContextValue(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD));
  }

  /**
   * Checks if segments generated by the insert query can be rolled up futher.
   *
   * @param query
   *
   * @return
   */
  private static boolean isRollupQuery(Query<?> query)
  {
    return TalariaContext.isFinalizeAggregations(query.getQueryContext()) == false
           && query.getContextBoolean(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, true) == false
           && query instanceof GroupByQuery;
  }

  private static boolean isInlineResults(final TalariaQuerySpec querySpec)
  {
    return querySpec.getDestination() instanceof TaskReportMSQDestination;
  }

  private static boolean isTimeBucketedIngestion(final TalariaQuerySpec querySpec)
  {
    return MSQControllerTask.isIngestion(querySpec)
           && !((DataSourceMSQDestination) querySpec.getDestination()).getSegmentGranularity()
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
    final int numShardColumns = clusterByColumns.size() - clusterBy.getBucketByCount() - (boosted ? 1 : 0);

    if (numShardColumns == 0) {
      return Collections.emptyList();
    }

    for (int i = clusterBy.getBucketByCount(); i < clusterBy.getBucketByCount() + numShardColumns; i++) {
      final ClusterByColumn column = clusterByColumns.get(i);
      final List<String> outputColumns = columnMappings.getOutputColumnsForQueryColumn(column.columnName());

      // DimensionRangeShardSpec only handles ascending order.
      if (column.descending()) {
        return Collections.emptyList();
      }

      ColumnType columnType = signature.getColumnType(column.columnName()).orElse(null);

      // DimensionRangeShardSpec only handles strings.
      if (!(ColumnType.STRING.equals(columnType))) {
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

  private static StringTuple makeStringTuple(
      final ClusterBy clusterBy,
      final ClusterByKeyReader keyReader,
      final ClusterByKey key
  )
  {
    final String[] array = new String[clusterBy.getColumns().size() - clusterBy.getBucketByCount()];
    final boolean boosted = isClusterByBoosted(clusterBy);

    for (int i = 0; i < array.length; i++) {
      final Object val = keyReader.read(key, clusterBy.getBucketByCount() + i);

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
      final List<String> segmentSortOrder,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final Query<?> query
  )
  {
    final List<DimensionSchema> dimensions = new ArrayList<>();
    final List<AggregatorFactory> aggregators = new ArrayList<>();

    // During ingestion, segment sort order is determined by the order of fields in the DimensionsSchema. We want
    // this to match user intent as dictated by the declared segment sort order and CLUSTERED BY, so add things in
    // that order.

    // Start with segmentSortOrder.
    final Set<String> outputColumnsInOrder = new LinkedHashSet<>(segmentSortOrder);

    // Then the query-level CLUSTERED BY.
    // Note: this doesn't work when CLUSTERED BY specifies an expression that is not being selected.
    for (final ClusterByColumn clusterByColumn : queryClusterBy.getColumns()) {
      if (clusterByColumn.descending()) {
        throw new TalariaException(new InsertCannotOrderByDescendingFault(clusterByColumn.columnName()));
      }

      outputColumnsInOrder.addAll(columnMappings.getOutputColumnsForQueryColumn(clusterByColumn.columnName()));
    }

    // Then all other columns.
    outputColumnsInOrder.addAll(columnMappings.getOutputColumnNames());

    Map<String, AggregatorFactory> outputColumnAggregatorFactories = new HashMap<>();

    if (isRollupQuery) {
      for (AggregatorFactory aggregatorFactory : ((GroupByQuery) query).getAggregatorSpecs()) {
        String outputColumn = Iterables.getOnlyElement(columnMappings.getOutputColumnsForQueryColumn(aggregatorFactory.getName()));
        if (outputColumnAggregatorFactories.containsKey(outputColumn)) {
          throw new ISE("There can only be one aggregator factory for column [%s].", outputColumn);
        } else {
          outputColumnAggregatorFactories.put(
              outputColumn,
              aggregatorFactory.withName(outputColumn).getCombiningFactory()
          );
        }
      }
    }

    // Each column can be of either time, dimension, aggregator. For this method. we can ignore the time column.
    // For non-complex columns, If the aggregator factory of the column is not available, we treat the column as
    // a dimension. For complex columns, certains hacks are in place.
    for (final String outputColumn : outputColumnsInOrder) {
      final String queryColumn = columnMappings.getQueryColumnForOutputColumn(outputColumn);
      final ColumnType type =
          querySignature.getColumnType(queryColumn)
                        .orElseThrow(() -> new ISE("No type for column [%s]", outputColumn));

      if (!outputColumn.equals(ColumnHolder.TIME_COLUMN_NAME)) {

        if (!type.is(ValueType.COMPLEX)) {
          // non complex columns
          populateDimensionsAndAggregators(
              dimensions,
              aggregators,
              outputColumnAggregatorFactories,
              outputColumn,
              type
          );
        } else {
          // complex columns only
          if (DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.containsKey(type.getComplexTypeName())) {
            dimensions.add(DimensionSchemaUtils.createDimensionSchema(outputColumn, type));
          } else if (!isRollupQuery) {
            aggregators.add(new PassthroughAggregatorFactory(outputColumn, type.getComplexTypeName()));
          } else {
            populateDimensionsAndAggregators(
                dimensions,
                aggregators,
                outputColumnAggregatorFactories,
                outputColumn,
                type
            );
          }
        }
      }
    }

    return Pair.of(dimensions, aggregators);
  }


  /**
   * If the output column is present in the outputColumnAggregatorFactories that means we already have the aggregator information for this column.
   * else treat this column as a dimension.
   *
   * @param dimensions                      list is poulated if the output col is deemed to be a dimension
   * @param aggregators                     list is populated with the aggregator if the output col is deemed to be a aggregation column.
   * @param outputColumnAggregatorFactories output col -> AggregatorFactory map
   * @param outputColumn                    column name
   * @param type                            columnType
   */
  private static void populateDimensionsAndAggregators(
      List<DimensionSchema> dimensions,
      List<AggregatorFactory> aggregators,
      Map<String, AggregatorFactory> outputColumnAggregatorFactories,
      String outputColumn,
      ColumnType type
  )
  {
    if (outputColumnAggregatorFactories.containsKey(outputColumn)) {
      aggregators.add(outputColumnAggregatorFactories.get(outputColumn));
    } else {
      dimensions.add(DimensionSchemaUtils.createDimensionSchema(outputColumn, type));
    }
  }

  private static DateTime getBucketDateTime(
      final ClusterByPartition partitionBoundary,
      final Granularity segmentGranularity,
      final ClusterByKeyReader keyReader
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return DateTimes.utc(0);
    } else {
      final ClusterByKey startKey = partitionBoundary.getStart();
      final DateTime timestamp =
          DateTimes.utc(TalariaTasks.primaryTimestampFromObjectForInsert(keyReader.read(startKey, 0)));

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

  private static TalariaStagesReport makeStageReport(
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final Map<Integer, Integer> stageWorkerCountMap,
      final Map<Integer, Integer> stagePartitionCountMap
  )
  {
    return TalariaStagesReport.create(
        queryDef,
        ImmutableMap.copyOf(stagePhaseMap),
        copyOfStageRuntimesEndingAtCurrentTime(stageRuntimeMap),
        stageWorkerCountMap,
        stagePartitionCountMap
    );
  }

  private static TalariaResultsReport makeResultsTaskReport(
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

    return new TalariaResultsReport(mappedSignature.build(), sqlTypeNames, resultsYielder);
  }

  private static TalariaStatusReport makeStatusReport(
      final TaskState taskState,
      @Nullable final MSQErrorReport errorReport,
      final Queue<MSQErrorReport> errorReports,
      @Nullable final DateTime queryStartTime,
      final long queryDuration
  )
  {
    return new TalariaStatusReport(taskState, errorReport, errorReports, queryStartTime, queryDuration);
  }

  private static InputSpecSlicerFactory makeInputSpecSlicerFactory(final DataSegmentTimelineView timelineView)
  {
    return stagePartitionsMap -> new MapInputSpecSlicer(
        ImmutableMap.<Class<? extends InputSpec>, InputSpecSlicer>builder()
                    .put(StageInputSpec.class, new StageInputSpecSlicer(stagePartitionsMap))
                    .put(ExternalInputSpec.class, new ExternalInputSpecSlicer())
                    .put(TableInputSpec.class, new TableInputSpecSlicer(timelineView))
                    .build()
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
          queryKernel.getActiveStages()
                     .stream()
                     .sorted(Comparator.comparing(id -> queryKernel.getStageDefinition(id).getStageNumber()))
                     .map(id -> StringUtils.format(
                              "%d:%d[%s:%s]>%s",
                              queryKernel.getStageDefinition(id).getStageNumber(),
                              queryKernel.getWorkerInputsForStage(id).workerCount(),
                              queryKernel.getStageDefinition(id).doesShuffle() ? "SHUFFLE" : "RETAIN",
                              queryKernel.getStagePhase(id),
                              queryKernel.doesStageHaveResultPartitions(id)
                              ? Iterators.size(queryKernel.getResultPartitionsForStage(id).iterator())
                              : "?"
                          )
                     )
                     .collect(Collectors.joining("; "))
      );
    }
  }

  /**
   * Method that determines whether an exception was raised due to the task lock for the leader task being
   * preempted by string comparison. Errors containing the following message return true
   * 1. {@link org.apache.druid.indexing.common.actions.TaskLocks} Segments[%s] are not covered by locks[%s] for task[%s]
   * 2. {@link SegmentAllocateAction} The lock for interval[%s] is preempted and no longer valid
   */
  private static boolean isTaskLockPreemptedException(Exception e)
  {
    final String exceptionMsg = e.getMessage();
    final List<String> validExceptionExcerpts = ImmutableList.of(
        "are not covered by locks",
        "is preempted and no longer valid"
    );
    return validExceptionExcerpts.stream().anyMatch(exceptionMsg::contains);
  }

  private void addToKernelManipulationQueue(Consumer<ControllerQueryKernel> kernelConsumer)
  {
    try {
      kernelManipulationQueue.add(kernelConsumer);
    }
    catch (IllegalStateException e) {
      log.warn("The leader's kernel manipulation queue is full. "
               + "To debug, start by taking stack traces of the leader and figure out which tasks are taking time.");
      throw new ISE("Leader's kernel manipulation queue is full. "
                    + "This indicates that leader kernel tasks are not completing fast enough.");
    }
    catch (Exception e) {
      throw e;
    }
  }

  private interface TaskContactFn
  {
    ListenableFuture<Void> contactTask(WorkerClient client, String taskId, int workerNumber);
  }

  @Override
  public RunningLeaderStatus status()
  {
    return new RunningLeaderStatus(id());
  }
}
