/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.kernel.StageId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Client for the Talaria Leader (Controller). Used by a Worker task.
 */
public interface LeaderClient extends AutoCloseable
{
  void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  ) throws IOException;
  void postCounters(CounterSnapshotsTree snapshotsTree) throws IOException;
  void postResultsComplete(
      StageId stageId,
      int workerNumber,
      @Nullable Object resultObject
  ) throws IOException;
  void postWorkerError(
      String workerId,
      MSQErrorReport errorWrapper
  ) throws IOException;

  void postWorkerWarning(
      String workerId,
      List<MSQErrorReport> MSQErrorReports
  ) throws IOException;
  List<String> getTaskList() throws IOException;

  @Override
  void close();
}
