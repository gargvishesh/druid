/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.worker;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.WorkOrder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public class WorkerStageKernel
{
  private final WorkOrder workOrder;

  private WorkerStagePhase phase = WorkerStagePhase.NEW;

  @Nullable
  private ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot;

  @Nullable
  private ClusterByPartitions resultPartitionBoundaries;

  @Nullable
  private Object resultObject;

  @Nullable
  private Throwable exceptionFromFail;

  private WorkerStageKernel(final WorkOrder workOrder)
  {
    this.workOrder = workOrder;

    if (workOrder.getStageDefinition().doesShuffle()
        && !workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      // Use valueOrThrow instead of a nicer error collection mechanism, because we really don't expect the
      // MAX_PARTITIONS to be exceeded here. It would involve having a shuffleSpec that was statically configured
      // to use a huge number of partitions.
      resultPartitionBoundaries = workOrder.getStageDefinition().generatePartitionsForShuffle(null).valueOrThrow();
    }
  }

  public static WorkerStageKernel create(final WorkOrder workOrder)
  {
    return new WorkerStageKernel(workOrder);
  }

  public WorkerStagePhase getPhase()
  {
    return phase;
  }

  public WorkOrder getWorkOrder()
  {
    return workOrder;
  }

  public StageDefinition getStageDefinition()
  {
    return workOrder.getStageDefinition();
  }

  public void startReading()
  {
    transitionTo(WorkerStagePhase.READING_INPUT);
  }

  public void startPreshuffleWaitingForResultPartitionBoundaries()
  {
    assertPreshuffleStatisticsNeeded();
    transitionTo(WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES);
  }

  public void startPreshuffleWritingOutput()
  {
    assertPreshuffleStatisticsNeeded();
    transitionTo(WorkerStagePhase.PRESHUFFLE_WRITING_OUTPUT);
  }

  public void setResultKeyStatisticsSnapshot(final ClusterByStatisticsSnapshot resultKeyStatisticsSnapshot)
  {
    assertPreshuffleStatisticsNeeded();
    this.resultKeyStatisticsSnapshot = resultKeyStatisticsSnapshot;
  }

  public void setResultPartitionBoundaries(final ClusterByPartitions resultPartitionBoundaries)
  {
    assertPreshuffleStatisticsNeeded();
    this.resultPartitionBoundaries = resultPartitionBoundaries;
  }

  public boolean hasResultKeyStatisticsSnapshot()
  {
    return resultKeyStatisticsSnapshot != null;
  }

  public boolean hasResultPartitionBoundaries()
  {
    return resultPartitionBoundaries != null;
  }

  public ClusterByStatisticsSnapshot getResultKeyStatisticsSnapshot()
  {
    return Preconditions.checkNotNull(resultKeyStatisticsSnapshot, "resultKeyStatisticsSnapshot");
  }

  public ClusterByPartitions getResultPartitionBoundaries()
  {
    return Preconditions.checkNotNull(resultPartitionBoundaries, "resultPartitionBoundaries");
  }

  @Nullable
  public Object getResultObject()
  {
    if (phase == WorkerStagePhase.RESULTS_READY) {
      return resultObject;
    } else {
      throw new ISE("Results are not ready yet");
    }
  }

  public Throwable getException()
  {
    if (phase == WorkerStagePhase.FAILED) {
      return exceptionFromFail;
    } else {
      throw new ISE("Stage has not failed");
    }
  }

  public void setResultsComplete(Object resultObject)
  {
    if (resultObject == null) {
      throw new NullPointerException("resultObject must not be null");
    }

    transitionTo(WorkerStagePhase.RESULTS_READY);
    this.resultObject = resultObject;
  }

  public void fail(Throwable t)
  {
    Preconditions.checkNotNull(t, "t");

    transitionTo(WorkerStagePhase.FAILED);
    resultKeyStatisticsSnapshot = null;
    resultPartitionBoundaries = null;

    if (exceptionFromFail == null) {
      exceptionFromFail = t;
    }
  }

  private void assertPreshuffleStatisticsNeeded()
  {
    if (!workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      throw new ISE(
          "Result partitioning is not necessary for stage [%s]",
          workOrder.getStageDefinition().getId()
      );
    }
  }

  private void transitionTo(final WorkerStagePhase newPhase)
  {
    if (newPhase.canTransitionFrom(phase)) {
      phase = newPhase;
    } else {
      throw new IAE("Cannot transition from [%s] to [%s]", phase, newPhase);
    }
  }
}
