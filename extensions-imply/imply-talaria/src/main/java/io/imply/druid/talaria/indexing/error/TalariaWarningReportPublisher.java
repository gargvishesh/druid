/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import io.imply.druid.talaria.exec.LeaderClient;

import javax.annotation.Nullable;

public class TalariaWarningReportPublisher
{
  final String leaderId;
  final String workerId;
  final String taskId;
  @Nullable
  final String host;
  final Integer stageNumber;
  final LeaderClient leaderClient;

  public TalariaWarningReportPublisher(
      final String leaderId,
      final String workerId,
      final LeaderClient leaderClient,
      final String taskId,
      @Nullable final String host,
      final Integer stageNumber
  )
  {
    this.leaderId = leaderId;
    this.workerId = workerId;
    this.leaderClient = leaderClient;
    this.taskId = taskId;
    this.host = host;
    this.stageNumber = stageNumber;
  }

  public void publishException(Throwable e)
  {
    // TODO: Chomp the exception stack trace if it is more than a predetermined size
    leaderClient.postWorkerWarning(leaderId, workerId, TalariaErrorReport.fromException(taskId, host, stageNumber, e));
  }
}
