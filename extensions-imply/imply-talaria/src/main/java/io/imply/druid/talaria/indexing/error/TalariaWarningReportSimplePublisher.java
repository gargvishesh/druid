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
 * TODO: Check if this is thread safe
 */
public class TalariaWarningReportSimplePublisher implements TalariaWarningReportPublisher
{

  final String leaderId;
  final String workerId;
  final LeaderClient leaderClient;
  final String taskId;
  @Nullable
  final String host;

  public TalariaWarningReportSimplePublisher(
      final String leaderId,
      final String workerId,
      final LeaderClient leaderClient,
      final String taskId,
      @Nullable final String host
  )
  {
    this.leaderId = leaderId;
    this.workerId = workerId;
    this.leaderClient = leaderClient;
    this.taskId = taskId;
    this.host = host;
  }


  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    TalariaErrorReport warningReport = TalariaErrorReport.fromException(taskId, host, stageNumber, e);
    leaderClient.postWorkerWarning(leaderId, workerId, ImmutableList.of(warningReport));
  }

  @Override
  public void close() throws IOException
  {

  }
}
