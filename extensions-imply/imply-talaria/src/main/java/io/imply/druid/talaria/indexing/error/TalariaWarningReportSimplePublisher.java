/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.collect.ImmutableList;
import io.imply.druid.talaria.exec.LeaderClient;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Publishes the warning report to the leader client as is without any buffering/batching
 */
public class TalariaWarningReportSimplePublisher implements TalariaWarningReportPublisher
{

  final String workerId;
  final LeaderClient leaderClient;
  final String taskId;
  @Nullable
  final String host;

  public TalariaWarningReportSimplePublisher(
      final String workerId,
      final LeaderClient leaderClient,
      final String taskId,
      @Nullable final String host
  )
  {
    this.workerId = workerId;
    this.leaderClient = leaderClient;
    this.taskId = taskId;
    this.host = host;
  }


  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    final MSQErrorReport warningReport = MSQErrorReport.fromException(taskId, host, stageNumber, e);

    try {
      leaderClient.postWorkerWarning(workerId, ImmutableList.of(warningReport));
    }
    catch (IOException e2) {
      throw new RuntimeException(e2);
    }
  }

  @Override
  public void close()
  {

  }
}
