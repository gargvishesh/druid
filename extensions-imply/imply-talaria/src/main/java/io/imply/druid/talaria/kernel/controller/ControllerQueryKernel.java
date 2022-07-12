/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.imply.druid.talaria.exec.QueryValidator;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.input.InputSpecSlicer;
import io.imply.druid.talaria.input.InputSpecSlicerFactory;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a thread-unsafe class
 * TODO(gianm): Put something in here to make sure that limited-size in-memory channels do not block
 *   (readers must be spun up at the same time as writers)
 */
public class ControllerQueryKernel
{
  private final QueryDefinition queryDef;
  private final Map<StageId, ControllerStageKernel> stageTrackers = new HashMap<>();

  private Map<StageId, Set<StageId>> inflowMap = null; // Will be initialized to an unmodifiable map
  private Map<StageId, Set<StageId>> outflowMap = null; // Will be initialized to an unmodifiable map

  // Maintains a running map of (stageId -> pending inflow stages) which need to be completed to provision the stage
  // corresponding to the stageId. After initializing,if the value of the entry becomes an empty set, it is removed from
  // the map, and the removed entry is added to readyToRunStages
  private Map<StageId, Set<StageId>> pendingInflowMap = null;

  // Maintains a running count of (stageId -> outflow stages pending on its results). After initializing, if
  // the value of the entry becomes an empty set, it is removed from the map and the removed entry is added to
  // effectivelyFinishedStages
  private Map<StageId, Set<StageId>> pendingOutflowMap = null;

  // Tracks those stages which can be initialized safely.
  private final Set<StageId> readyToRunStages = new HashSet<>();

  // Tracks the stageIds which can be finished. Once returned by getEffectivelyFinishedStageKernels(), it gets cleared
  // and not tracked anymore in this Set
  private final Set<StageId> effectivelyFinishedStages = new HashSet<>(); // Modifiable map

  public ControllerQueryKernel(final QueryDefinition queryDef)
  {
    this.queryDef = queryDef;
    initializeStageDAGMaps();
    initializeLeafStages();
  }

  /**
   * Creates new kernels, if they can be initialized, and returns the tracked kernels which are in NEW phase
   */
  public List<StageId> createAndGetNewStageIds(
      final InputSpecSlicerFactory slicerFactory,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    final Int2IntMap stageWorkerCountMap = new Int2IntAVLTreeMap();
    final Int2ObjectMap<ReadablePartitions> stagePartitionsMap = new Int2ObjectAVLTreeMap<>();

    for (final ControllerStageKernel stageKernel : stageTrackers.values()) {
      final int stageNumber = stageKernel.getStageDefinition().getStageNumber();
      stageWorkerCountMap.put(stageNumber, stageKernel.getWorkerInputs().workerCount());

      if (stageKernel.hasResultPartitions()) {
        stagePartitionsMap.put(stageNumber, stageKernel.getResultPartitions());
      }
    }

    createNewKernels(stageWorkerCountMap, slicerFactory.makeSlicer(stagePartitionsMap), assignmentStrategy);
    return stageTrackers.values()
                        .stream()
                        .filter(controllerStageKernel -> controllerStageKernel.getPhase() == ControllerStagePhase.NEW)
                        .map(stageKernel -> stageKernel.getStageDefinition().getId())
                        .collect(Collectors.toList());
  }

  /**
   * @return Stage kernels in this query kernel which can be safely cleaned up and marked as FINISHED. This returns the
   * kernel corresponding to a particular stage only once, to reduce the number of stages to iterate through.
   * It is expectant of the caller to eventually mark the stage as {@link ControllerStagePhase#FINISHED} after fetching
   * the stage kernel
   */
  public List<StageId> getEffectivelyFinishedStageIds()
  {
    return ImmutableList.copyOf(effectivelyFinishedStages);
  }

  /**
   * Returns all the kernels which have been initialized and are being tracked
   */
  public List<StageId> getActiveStages()
  {
    return ImmutableList.copyOf(stageTrackers.keySet());
  }

  /**
   * Returns a stage's kernel corresponding to a particular stage number
   */
  public StageId getStageId(final int stageNumber)
  {
    return new StageId(queryDef.getQueryId(), stageNumber);
  }

  /**
   * Returns true if query needs no further processing, i.e. if final stage is successful or if any of the stages have
   * been failed
   */
  public boolean isDone()
  {
    return Optional.ofNullable(stageTrackers.get(queryDef.getFinalStageDefinition().getId()))
                   .filter(tracker -> ControllerStagePhase.isSuccessfulTerminalPhase(tracker.getPhase()))
                   .isPresent()
           || stageTrackers.values().stream().anyMatch(tracker -> tracker.getPhase() == ControllerStagePhase.FAILED);
  }

  /**
   * Marks all the successful terminal stages to completion, so that the queryKernel shows a canonical view of
   * phases of the stages once it completes
   */
  public void markSuccessfulTerminalStagesAsFinished()
  {
    for (final StageId stageId : getActiveStages()) {
      ControllerStagePhase phase = getStagePhase(stageId);
      // While the following conditional is redundant currently, it makes logical sense to mark all the "successful
      // terminal phases" to FINISHED at the end, hence the if clause. Inside the conditional, depending on the
      // terminal phase it resides in, we synthetically mark it to completion (and therefore we need to check which
      // stage it is precisely in)
      if (ControllerStagePhase.isSuccessfulTerminalPhase(phase)) {
        if (phase == ControllerStagePhase.RESULTS_READY) {
          finishStage(stageId, false);
        }
      }
    }
  }

  /**
   * Returns true if all the stages comprising the query definition have been sucessful in producing their results
   */
  public boolean isSuccess()
  {
    return stageTrackers.size() == queryDef.getStageDefinitions().size()
           && stageTrackers.values()
                           .stream()
                           .allMatch(tracker -> ControllerStagePhase.isSuccessfulTerminalPhase(tracker.getPhase()));
  }

  /**
   * Creates a list of work orders, corresponding to each worker, for a particular stageNumber
   */
  public Int2ObjectMap<WorkOrder> createWorkOrders(
      final int stageNumber,
      @Nullable final Int2ObjectMap<Object> extraInfos
  )
  {
    final Int2ObjectMap<WorkOrder> retVal = new Int2ObjectAVLTreeMap<>();
    final ControllerStageKernel stageKernel = getStageKernelOrThrow(getStageId(stageNumber));

    final WorkerInputs workerInputs = stageKernel.getWorkerInputs();
    for (int workerNumber : workerInputs.workers()) {
      final Object extraInfo = extraInfos != null ? extraInfos.get(workerNumber) : null;

      //noinspection unchecked
      final ExtraInfoHolder<?> extraInfoHolder =
          stageKernel.getStageDefinition().getProcessorFactory().makeExtraInfoHolder(extraInfo);

      final WorkOrder workOrder = new WorkOrder(
          queryDef,
          stageNumber,
          workerNumber,
          workerInputs.inputsForWorker(workerNumber),
          extraInfoHolder
      );

      QueryValidator.validateWorkOrder(workOrder);
      retVal.put(workerNumber, workOrder);
    }

    return retVal;
  }

  private void createNewKernels(
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    for (final StageId nextStage : readyToRunStages) {
      // Create a tracker.
      final StageDefinition stageDef = queryDef.getStageDefinition(nextStage);
      final ControllerStageKernel stageKernel = ControllerStageKernel.create(
          stageDef,
          stageWorkerCountMap,
          slicer,
          assignmentStrategy
      );
      stageTrackers.put(nextStage, stageKernel);
    }

    readyToRunStages.clear();
  }

  /**
   * Populates the inflowMap, outflowMap and pending inflow/outflow maps corresponding to the query definition
   */
  private void initializeStageDAGMaps()
  {
    initializeStageOutflowMap(this.queryDef);
    initializeStageInflowMap(this.queryDef);
  }

  /**
   * Initializes this.outflowMap with a mapping of stage -> stages that depend on that stage.
   */
  private void initializeStageOutflowMap(final QueryDefinition queryDefinition)
  {
    Preconditions.checkArgument(this.outflowMap == null, "outflow map must only be built once");
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();
    this.pendingOutflowMap = new HashMap<>();
    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new HashSet<>());
      this.pendingOutflowMap.computeIfAbsent(stageId, ignored -> new HashSet<>());
      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDef.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(inputStageId, ignored -> new HashSet<>()).add(stageId);
        this.pendingOutflowMap.computeIfAbsent(inputStageId, ignored -> new HashSet<>()).add(stageId);
      }
    }
    this.outflowMap = Collections.unmodifiableMap(retVal);
  }

  /**
   * Initializes this.inflowMap with a mapping of stage -> stages that flow *into* that stage.
   */
  private void initializeStageInflowMap(final QueryDefinition queryDefinition)
  {
    final Map<StageId, Set<StageId>> retVal = new HashMap<>();
    this.pendingInflowMap = new HashMap<>();
    for (final StageDefinition stageDef : queryDefinition.getStageDefinitions()) {
      final StageId stageId = stageDef.getId();
      retVal.computeIfAbsent(stageId, ignored -> new HashSet<>());
      this.pendingInflowMap.computeIfAbsent(stageId, ignored -> new HashSet<>());
      for (final int inputStageNumber : queryDefinition.getStageDefinition(stageId).getInputStageNumbers()) {
        final StageId inputStageId = new StageId(queryDef.getQueryId(), inputStageNumber);
        retVal.computeIfAbsent(stageId, ignored -> new HashSet<>()).add(inputStageId);
        this.pendingInflowMap.computeIfAbsent(stageId, ignored -> new HashSet<>()).add(inputStageId);
      }
    }
    this.inflowMap = Collections.unmodifiableMap(retVal);
  }

  /**
   * Adds stageIds for those stages which donot require any input from any other stages
   */
  private void initializeLeafStages()
  {
    Iterator<Map.Entry<StageId, Set<StageId>>> pendingInflowIterator = pendingInflowMap.entrySet().iterator();
    while (pendingInflowIterator.hasNext()) {
      Map.Entry<StageId, Set<StageId>> stageToInflowStages = pendingInflowIterator.next();
      if (stageToInflowStages.getValue().size() == 0) {
        readyToRunStages.add(stageToInflowStages.getKey());
        pendingInflowIterator.remove();
      }
    }
  }

  // Following section contains the methods which delegate to appropriate stage kernel

  /**
   * Delegates call to {@link ControllerStageKernel#getStageDefinition()}
   */
  public StageDefinition getStageDefinition(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getStageDefinition();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getPhase()}
   */
  public ControllerStagePhase getStagePhase(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getPhase();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#canReadResults()}
   */
  public boolean canStageReadResults(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).canReadResults();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#hasResultPartitions()}
   */
  public boolean doesStageHaveResultPartitions(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).hasResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getResultPartitions()}
   */
  public ReadablePartitions getResultPartitionsForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultPartitions();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getResultPartitionBoundaries()}
   */
  public ClusterByPartitions getResultPartitionBoundariesForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultPartitionBoundaries();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#collectorEncounteredAnyMultiValueField()}
   */
  public boolean hasStageCollectorEncounteredAnyMultiValueField(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).collectorEncounteredAnyMultiValueField();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getResultObject()}
   */
  public Object getResultObjectForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getResultObject();
  }

  /**
   * Checks if the stage can be started, delegates call to {@link ControllerStageKernel#start()} for internal phase
   * transition and registers the transition in this queryKernel
   */
  public void startStage(final StageId stageId)
  {
    final ControllerStageKernel stageKernel = getStageKernelOrThrow(stageId);
    if (stageKernel.getPhase() != ControllerStagePhase.NEW) {
      throw new ISE("Cannot start the stage: [%s]", stageId);
    }
    stageKernel.start();
    transitionStageKernel(stageId, ControllerStagePhase.READING_INPUT);
  }

  /**
   * Checks if the stage can be finished, delegates call to {@link ControllerStageKernel#finish()} for internal phase
   * transition and registers the transition in this query kernel
   *
   * If the method is called with strict = true, we confirm if the stage can be marked as finished or else
   * throw illegal argument exception
   */
  public void finishStage(final StageId stageId, final boolean strict)
  {
    if (strict && !effectivelyFinishedStages.contains(stageId)) {
      throw new IAE("Cannot mark the stage: [%s] finished", stageId);
    }
    getStageKernelOrThrow(stageId).finish();
    effectivelyFinishedStages.remove(stageId);
    transitionStageKernel(stageId, ControllerStagePhase.FINISHED);
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getWorkerInputs()}
   */
  public WorkerInputs getWorkerInputsForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getWorkerInputs();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#addResultKeyStatisticsForWorker(int, ClusterByStatisticsSnapshot)}.
   * If calling this causes transition for the stage kernel, then this gets registered in this query kernel
   */
  public void addResultKeyStatisticsForStageAndWorker(
      final StageId stageId,
      final int workerNumber,
      final ClusterByStatisticsSnapshot snapshot
  )
  {
    ControllerStagePhase newPhase = getStageKernelOrThrow(stageId).addResultKeyStatisticsForWorker(
        workerNumber,
        snapshot
    );

    // If the phase is POST_READING or FAILED, that implies the kernel has transitioned. We need to account for that
    switch (newPhase) {
      case POST_READING:
      case FAILED:
        transitionStageKernel(stageId, newPhase);
        break;
    }
  }

  /**
   * Delegates call to {@link ControllerStageKernel#setResultsCompleteForWorker(int, Object)}. If calling this causes
   * transition for the stage kernel, then this gets registered in this query kernel
   */
  public void setResultsCompleteForStageAndWorker(
      final StageId stageId,
      final int workerNumber,
      final Object resultObject
  )
  {
    if (getStageKernelOrThrow(stageId).setResultsCompleteForWorker(workerNumber, resultObject)) {
      transitionStageKernel(stageId, ControllerStagePhase.RESULTS_READY);
    }
  }

  /**
   * Delegates call to {@link ControllerStageKernel#getFailureReason()}
   */
  public ControllerStageFailureReason getFailureReasonForStage(final StageId stageId)
  {
    return getStageKernelOrThrow(stageId).getFailureReason();
  }

  /**
   * Delegates call to {@link ControllerStageKernel#fail()} and registers this transition to FAILED in this query kernel
   */
  public void failStage(final StageId stageId)
  {
    getStageKernelOrThrow(stageId).fail();
    transitionStageKernel(stageId, ControllerStagePhase.FAILED);
  }

  /**
   * Fetches and returns the stage kernel corresponding to the provided stage id, else throws {@link IAE}
   */
  private ControllerStageKernel getStageKernelOrThrow(StageId stageId)
  {
    ControllerStageKernel stageKernel = stageTrackers.get(stageId);
    if (stageKernel == null) {
      throw new IAE("Cannot find kernel corresponding to stage [%s] in query [%s]", stageId, queryDef.getQueryId());
    }
    return stageKernel;
  }

  /**
   * Whenever a stage kernel changes it phase, the change must be "registered" by calling this method with the stageId
   * and the new phase
   */
  public void transitionStageKernel(StageId stageId, ControllerStagePhase newPhase)
  {

    Preconditions.checkArgument(
        stageTrackers.containsKey(stageId),
        "Attempting to modify an unknown stageKernel"
    );

    if (newPhase == ControllerStagePhase.RESULTS_READY) {
      // Once the stage has produced its results, we remove it from all the stages depending on this stage (for its
      // output).
      for (StageId dependentStageId : outflowMap.get(stageId)) {
        if (!pendingInflowMap.containsKey(dependentStageId)) {
          continue;
        }
        pendingInflowMap.get(dependentStageId).remove(stageId);
        // Check the dependent stage. If it has no dependencies left, it can be marked as to be initialized
        if (pendingInflowMap.get(dependentStageId).size() == 0) {
          readyToRunStages.add(dependentStageId);
          pendingInflowMap.remove(dependentStageId);
        }
      }
    }

    if (ControllerStagePhase.isPostReadingPhase(newPhase)) {
      // Once the stage has consumed all the data/input from its dependent stages, we remove it from all the stages
      // whose input it was dependent on
      for (StageId inputStage : inflowMap.get(stageId)) {
        if (!pendingOutflowMap.containsKey(inputStage)) {
          continue;
        }
        pendingOutflowMap.get(inputStage).remove(stageId);
        // If no more stage is dependent on the "inputStage's" results, it can be safely transitioned to FINISHED
        if (pendingOutflowMap.get(inputStage).size() == 0) {
          effectivelyFinishedStages.add(inputStage);
          pendingOutflowMap.remove(inputStage);
        }
      }
    }
  }

  @VisibleForTesting
  ControllerStageKernel getControllerStageKernel(int stageNumber)
  {
    return stageTrackers.get(new StageId(queryDef.getQueryId(), stageNumber));
  }
}
