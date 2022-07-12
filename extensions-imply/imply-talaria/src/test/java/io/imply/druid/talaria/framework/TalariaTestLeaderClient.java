/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderClient;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.kernel.StageId;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.List;

public class TalariaTestLeaderClient implements LeaderClient
{
  private final Leader leader;

  public TalariaTestLeaderClient(Leader leader)
  {
    this.leader = leader;
  }

  @Override
  public void postKeyStatistics(
      StageId stageId,
      int workerNumber,
      ClusterByStatisticsSnapshot keyStatistics
  )
  {
    try {
      leader.updateStatus(stageId.getStageNumber(), workerNumber, keyStatistics);
    }
    catch (Exception e) {
      throw new ISE(e, "unable to post key statistics");
    }
  }

  @Override
  public void postCounters(CounterSnapshotsTree snapshotsTree)
  {
    if (snapshotsTree != null) {
      leader.updateCounters(snapshotsTree);
    }
  }

  @Override
  public void postResultsComplete(StageId stageId, int workerNumber, @Nullable Object resultObject)
  {
    leader.resultsComplete(stageId.getQueryId(), stageId.getStageNumber(), workerNumber, resultObject);
  }

  @Override
  public void postWorkerError(String workerId, MSQErrorReport errorWrapper)
  {
    leader.workerError(errorWrapper);
  }

  @Override
  public void postWorkerWarning(String workerId, List<MSQErrorReport> MSQErrorReports)
  {
    leader.workerWarning(MSQErrorReports);
  }

  @Override
  public List<String> getTaskList()
  {
    return leader.getTaskIds();
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
