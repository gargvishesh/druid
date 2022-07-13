/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSpecSlicer;
import io.imply.druid.talaria.input.StageInputSlice;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Kernel for each stage. This is a package-private class because a stage kernel must be owned by a
 * {@link ControllerQueryKernel} and all the methods of this class must be invoked via the corresponding methods of
 * {@link ControllerQueryKernel}. This is to ensure that a query has a global view of all the phases of its stages.
 *
 * TODO(gianm): handle worker failures by tracking phase for each indiv. worker & allowing phase rollback
 */
class ControllerStageKernel
{
  private final StageDefinition stageDef;
  private final int workerCount;

  private final WorkerInputs workerInputs;
  private final IntSet workersWithResultKeyStatistics = new IntAVLTreeSet();
  private final IntSet workersWithResultsComplete = new IntAVLTreeSet();

  private ControllerStagePhase phase = ControllerStagePhase.NEW;

  @Nullable
  private final ClusterByStatisticsCollector resultKeyStatisticsCollector;

  // Result partitions and where they can be read from.
  @Nullable
  private ReadablePartitions resultPartitions;

  // Boundaries for the result partitions. Only set if this stage is shuffling.
  @Nullable
  private ClusterByPartitions resultPartitionBoundaries;

  @Nullable
  private Object resultObject;

  @Nullable // Set if phase is FAILED
  private ControllerStageFailureReason failureReason;

  private ControllerStageKernel(
      final StageDefinition stageDef,
      final WorkerInputs workerInputs
  )
  {
    this.stageDef = stageDef;
    this.workerCount = workerInputs.workerCount();
    this.workerInputs = workerInputs;

    if (stageDef.mustGatherResultKeyStatistics()) {
      this.resultKeyStatisticsCollector = stageDef.createResultKeyStatisticsCollector();
    } else {
      this.resultKeyStatisticsCollector = null;
      generateResultPartitionsAndBoundaries();
    }
  }

  /**
   * TODO(gianm): Javadoc. Note that this is the method that decides how many workers to use.
   */
  static ControllerStageKernel create(
      final StageDefinition stageDef,
      final Int2IntMap stageWorkerCountMap,
      final InputSpecSlicer slicer,
      final WorkerAssignmentStrategy assignmentStrategy
  )
  {
    final WorkerInputs workerInputs = WorkerInputs.create(stageDef, stageWorkerCountMap, slicer, assignmentStrategy);
    return new ControllerStageKernel(stageDef, workerInputs);
  }

  /**
   * @return StageDefinition associated with the stage represented by this kernel
   */
  StageDefinition getStageDefinition()
  {
    return stageDef;
  }

  /**
   * @return The phase this stageKernel is in
   */
  ControllerStagePhase getPhase()
  {
    return phase;
  }

  /**
   * @return true if the results of this stage are ready for consumption i.e. the corresponding phase is RESULTS_READY
   */
  boolean canReadResults()
  {
    // TODO(gianm): want to support running streamable stages in a pipeline, but can't yet since links between
    //   stages are done as files on the worker side
    return phase == ControllerStagePhase.RESULTS_READY;
  }

  /**
   * @return if partitions for the results of this stage have been set
   */
  boolean hasResultPartitions()
  {
    return resultPartitions != null;
  }

  /**
   * @return partitions for the results of the stage associated with this kernel
   */
  ReadablePartitions getResultPartitions()
  {
    if (resultPartitions == null) {
      throw new ISE("Result partition information is not ready yet");
    } else {
      return resultPartitions;
    }
  }

  /**
   * @return Partition boundaries for the results of this stage
   */
  ClusterByPartitions getResultPartitionBoundaries()
  {
    if (!getStageDefinition().doesShuffle()) {
      throw new ISE("Result partition information is not relevant to this stage because it does not shuffle");
    } else if (resultPartitionBoundaries == null) {
      throw new ISE("Result partition information is not ready yet");
    } else {
      return resultPartitionBoundaries;
    }
  }

  /**
   * Whether the result key statistics collector for this stage has encountered any multi-valued input at
   * any key position.
   *
   * This method exists because {@link org.apache.druid.timeline.partition.DimensionRangeShardSpec} does not
   * support partitioning on multi-valued strings, so we need to know if any multi-valued strings exist in order
   * to decide whether we can use this kind of shard spec.
   */
  boolean collectorEncounteredAnyMultiValueField()
  {
    if (resultKeyStatisticsCollector == null) {
      throw new ISE("Stage does not gather result key statistics");
    } else if (resultPartitions == null) {
      throw new ISE("Result key statistics are not ready");
    } else {
      for (int i = 0; i < resultKeyStatisticsCollector.getClusterBy().getColumns().size(); i++) {
        if (resultKeyStatisticsCollector.hasMultipleValues(i)) {
          return true;
        }
      }

      return false;
    }
  }

  /**
   * @return Result object associated with this stage
   */
  Object getResultObject()
  {
    if (phase == ControllerStagePhase.FINISHED) {
      throw new ISE("Result object has been cleaned up prematurely");
    } else if (phase != ControllerStagePhase.RESULTS_READY) {
      throw new ISE("Result object is not ready yet");
    } else if (resultObject == null) {
      throw new NullPointerException("resultObject was unexpectedly null");
    } else {
      return resultObject;
    }
  }

  /**
   * Marks that the stage is no longer NEW and has started reading inputs (and doing work)
   */
  void start()
  {
    transitionTo(ControllerStagePhase.READING_INPUT);
  }

  /**
   * Marks that the stage is finished and its results must not be used as they could have cleaned up.
   */
  void finish()
  {
    transitionTo(ControllerStagePhase.FINISHED);
  }

  /**
   * Inputs to each worker for this particular stage.
   */
  WorkerInputs getWorkerInputs()
  {
    return workerInputs;
  }

  /**
   * Adds result key statistics for a particular worker number. If statistics have already been added for this worker,
   * then this call ignores the new ones and does nothing.
   *
   * @param workerNumber the worker
   * @param snapshot     worker statistics
   */
  ControllerStagePhase addResultKeyStatisticsForWorker(
      final int workerNumber,
      final ClusterByStatisticsSnapshot snapshot
  )
  {
    if (resultKeyStatisticsCollector == null) {
      throw new ISE("Stage does not gather result key statistics");
    }

    if (workerNumber < 0 || workerNumber >= workerCount) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    if (phase != ControllerStagePhase.READING_INPUT) {
      throw new ISE("Cannot add result key statistics from stage [%s]", phase);
    }

    try {
      if (workersWithResultKeyStatistics.add(workerNumber)) {
        resultKeyStatisticsCollector.addAll(snapshot);

        if (workersWithResultKeyStatistics.size() == workerCount) {
          generateResultPartitionsAndBoundaries();

          // Phase can become FAILED after generateResultPartitionsAndBoundaries, if there were too many partitions.
          if (phase != ControllerStagePhase.FAILED) {
            transitionTo(ControllerStagePhase.POST_READING);
          }
        }
      }
    }
    catch (Exception e) {
      // If this op fails, we're in an inconsistent state and must cancel the stage.
      fail();
      throw e;
    }
    return getPhase();
  }

  /**
   * Accepts and sets the results that each worker produces for this particular stage
   *
   * @return true if the results for this stage have been gathered from all the workers, else false
   */
  @SuppressWarnings("unchecked")
  boolean setResultsCompleteForWorker(final int workerNumber, final Object resultObject)
  {
    if (workerNumber < 0 || workerNumber >= workerCount) {
      throw new IAE("Invalid workerNumber [%s]", workerNumber);
    }

    if (resultObject == null) {
      throw new NullPointerException("resultObject must not be null");
    }

    // TODO(gianm): Allow rolling back workers from 'ready' to... not ready... if they fail.
    // TODO(gianm): Allow rolling back phase if worker(s) fail and results are lost.
    if (workersWithResultsComplete.add(workerNumber)) {
      if (this.resultObject == null) {
        this.resultObject = resultObject;
      } else {
        //noinspection unchecked
        this.resultObject = getStageDefinition().getProcessorFactory()
                                                .mergeAccumulatedResult(this.resultObject, resultObject);
      }
    }

    if (workersWithResultsComplete.size() == workerCount) {
      transitionTo(ControllerStagePhase.RESULTS_READY);
      return true;
    }
    return false;
  }

  /**
   * @return Reason for failure of the stage
   */
  ControllerStageFailureReason getFailureReason()
  {
    if (phase != ControllerStagePhase.FAILED) {
      throw new ISE("No failure");
    }

    return failureReason;
  }

  /**
   * Marks the stage as failed, and sets the reason to {@code OTHER}
   */
  void fail()
  {
    failForReason(ControllerStageFailureReason.OTHER);
  }

  /**
   * TODO(gianm): Javadoc about preconditions
   */
  private void generateResultPartitionsAndBoundaries()
  {
    if (resultPartitions != null) {
      throw new ISE("Result partitions have already been generated");
    }

    final int stageNumber = stageDef.getStageNumber();

    if (stageDef.doesShuffle()) {
      if (stageDef.mustGatherResultKeyStatistics() && workersWithResultKeyStatistics.size() != workerCount) {
        throw new ISE("Cannot generate result partitions without all worker statistics");
      }

      final Either<Long, ClusterByPartitions> maybeResultPartitionBoundaries =
          stageDef.generatePartitionsForShuffle(resultKeyStatisticsCollector);

      if (maybeResultPartitionBoundaries.isError()) {
        failForReason(ControllerStageFailureReason.TOO_MANY_PARTITIONS);
        return;
      }

      resultPartitionBoundaries = maybeResultPartitionBoundaries.valueOrThrow();
      resultPartitions = ReadablePartitions.striped(
          stageNumber,
          workerCount,
          resultPartitionBoundaries.size()
      );
    } else {
      // No reshuffling: retain partitioning from nonbroadcast inputs.
      final Int2IntSortedMap partitionToWorkerMap = new Int2IntAVLTreeMap();
      for (int workerNumber : workerInputs.workers()) {
        final List<InputSlice> slices = workerInputs.inputsForWorker(workerNumber);
        for (int inputNumber = 0; inputNumber < slices.size(); inputNumber++) {
          final InputSlice slice = slices.get(inputNumber);

          if (slice instanceof StageInputSlice && !stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
            final StageInputSlice stageInputSlice = (StageInputSlice) slice;
            for (final ReadablePartition partition : stageInputSlice.getPartitions()) {
              partitionToWorkerMap.put(partition.getPartitionNumber(), workerNumber);
            }
          }
        }
      }

      resultPartitions = ReadablePartitions.collected(stageNumber, partitionToWorkerMap);
    }
  }

  /**
   * Marks the stage as failed and sets the reason for the same
   *
   * @param reason reason for which this stage has failed
   */
  private void failForReason(final ControllerStageFailureReason reason)
  {
    transitionTo(ControllerStagePhase.FAILED);

    this.failureReason = reason;

    if (resultKeyStatisticsCollector != null) {
      resultKeyStatisticsCollector.clear();
    }
  }

  void transitionTo(final ControllerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      phase = newPhase;
    } else {
      throw new IAE("Cannot transition from [%s] to [%s]", phase, newPhase);
    }
  }
}
