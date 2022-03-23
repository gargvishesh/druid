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
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

  // worker number -> inputs for that worker
  private final List<ReadablePartitions> workerInputs;
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
      final List<ReadablePartitions> workerInputs
  )
  {
    this.stageDef = stageDef;
    this.workerCount = workerInputs.size();
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
      final List<ControllerStageKernel> inputTrackers
  )
  {
    // Sanity check.
    if (stageDef.getInputStageIds().size() != inputTrackers.size()) {
      throw new IAE(
          "Expected [%d] input stage trackers, but got [%d]",
          stageDef.getInputStageIds().size(),
          inputTrackers.size()
      );
    }

    // Sanity check.
    for (int i = 0; i < inputTrackers.size(); i++) {
      final StageId expectedStage = stageDef.getInputStageIds().get(i);
      final StageId gotStage = inputTrackers.get(i).getStageDefinition().getId();

      if (!gotStage.equals(expectedStage)) {
        throw new IAE(
            "Expected stage [%s] at position [%d], but got [%d]",
            expectedStage.getStageNumber(),
            i,
            gotStage.getStageNumber()
        );
      }
    }

    // Decide how many workers to use, and assign inputs.
    final List<ReadablePartitions> workerInputs;

    if (inputTrackers.size() == 0) {
      workerInputs = new ArrayList<>(stageDef.getMaxWorkerCount());

      for (int i = 0; i < stageDef.getMaxWorkerCount(); i++) {
        workerInputs.add(ReadablePartitions.empty());
      }
    } else {
      // Input stage index (not stage number!) -> splits for that input stage.
      final List<List<ReadablePartitions>> splits = new ArrayList<>();

      // Populate "splits" with nulls.
      for (@SuppressWarnings("unused") final ControllerStageKernel ignored : inputTrackers) {
        splits.add(null);
      }

      // Populate "splits" with all non-broadcast inputs.
      for (int i = 0; i < inputTrackers.size(); i++) {
        final ControllerStageKernel tracker = inputTrackers.get(i);

        if (!stageDef.getBroadcastInputStageIds().contains(tracker.getStageDefinition().getId())) {
          splits.set(i, tracker.getResultPartitions().split(stageDef.getMaxWorkerCount()));
        }
      }

      // Decide how many workers there will be, based on the number of non-broadcast splits.
      // If there is no non-broadcast data, then run the maximum number of workers.
      // TODO(gianm): Of course, this is not a good idea if the *only* inputs are broadcast (like joining a query with
      //   a query). But it's important when there are "phantom" inputs like external data or Druid tables.
      final int actualNumWorkers =
          splits.stream().filter(Objects::nonNull).mapToInt(List::size).max().orElse(stageDef.getMaxWorkerCount());

      // Populate "splits" with all broadcast inputs.
      for (int i = 0; i < inputTrackers.size(); i++) {
        final ControllerStageKernel tracker = inputTrackers.get(i);

        if (stageDef.getBroadcastInputStageIds().contains(tracker.getStageDefinition().getId())) {
          final List<ReadablePartitions> trackerSplits = new ArrayList<>();
          final ReadablePartitions resultPartitions = tracker.getResultPartitions();

          for (int workerNumber = 0; workerNumber < actualNumWorkers; workerNumber++) {
            trackerSplits.add(resultPartitions);
          }

          splits.set(i, trackerSplits);
        }
      }

      // Flip the splits, so it's worker number -> splits for that worker.
      workerInputs = new ArrayList<>();

      for (int workerNumber = 0; workerNumber < actualNumWorkers; workerNumber++) {
        final List<ReadablePartitions> workerStageInputs = new ArrayList<>();

        for (final List<ReadablePartitions> split : splits) {
          if (split.size() > workerNumber) {
            workerStageInputs.add(split.get(workerNumber));
          }
        }

        workerInputs.add(ReadablePartitions.combine(workerStageInputs));
      }
    }

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
   * TODO(gianm): hack alert: see note for hasMultipleValues in ClusterByStatisticsCollectorImpl
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
   * @return Inputs to each worker for this particular stage, represented as ReadablePartitions. As a convention,
   * i-th readable partition represents the input to i-th worker.
   */
  List<ReadablePartitions> getWorkerInputs()
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
  ControllerStagePhase addResultKeyStatisticsForWorker(final int workerNumber, final ClusterByStatisticsSnapshot snapshot)
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
      final Int2IntSortedMap partitionToWorkerMap = new Int2IntAVLTreeMap();
      for (int workerNumber = 0; workerNumber < workerInputs.size(); workerNumber++) {
        final ReadablePartitions workerInput = workerInputs.get(workerNumber);

        for (ReadablePartition partition : workerInput) {
          partitionToWorkerMap.put(partition.getPartitionNumber(), workerNumber);
        }
      }

      resultPartitions = ReadablePartitions.collected(stageNumber, partitionToWorkerMap);
    }
  }

  /**
   * Marks the stage as failed and sets the reason for the same
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
