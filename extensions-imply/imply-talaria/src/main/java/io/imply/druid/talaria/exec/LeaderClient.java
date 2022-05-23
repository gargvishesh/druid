/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.kernel.StageId;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * Client for the Talaria Leader (Controller). Used by a Worker task.
 */
public interface LeaderClient extends AutoCloseable
{
  void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  );
  void postCounters(
      String workerId,
      TalariaCountersSnapshot.WorkerCounters snapshot
  );
  void postResultsComplete(
      StageId stageId,
      int workerNumber,
      @Nullable Object resultObject
  );
  void postWorkerError(
      String workerId,
      TalariaErrorReport errorWrapper
  );

  void postWorkerWarning(
      String workerId,
      List<TalariaErrorReport> talariaErrorReports
  );
  Optional<List<String>> getTaskList();

  @Override
  void close();
}
