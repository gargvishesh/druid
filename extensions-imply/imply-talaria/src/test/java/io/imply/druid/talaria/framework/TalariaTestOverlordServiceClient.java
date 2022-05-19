/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderImpl;
import io.imply.druid.talaria.indexing.TalariaControllerTask;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import io.imply.druid.talaria.rpc.RetryPolicy;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TalariaTestOverlordServiceClient implements OverlordServiceClient
{
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final TaskActionClient taskActionClient;
  private Map<String, Leader> inMemoryLeaders = new HashMap<>();
  private Map<String, Map<String, TaskReport>> reports = new HashMap<>();
  private Map<String, TalariaQuerySpec> talariaQuerySpec = new HashMap<>();

  public TalariaTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    TaskStatus status;
    LeaderImpl leader = null;
    TalariaTestLeaderContext talariaTestLeaderContext = null;
    try {
      talariaTestLeaderContext = new TalariaTestLeaderContext(objectMapper, injector, taskActionClient);

      TalariaControllerTask cTask = objectMapper.convertValue(taskObject, TalariaControllerTask.class);
      talariaQuerySpec.put(cTask.getId(), cTask.getQuerySpec());

      leader = new LeaderImpl(
          cTask,
          talariaTestLeaderContext
      );

      inMemoryLeaders.put(cTask.getId(), leader);

      leader.run();
      return Futures.immediateFuture(null);
    }
    catch (Exception exception) {
      throw new ISE(exception.getCause(), "Unable to run");
    }
    finally {
      if (leader != null && talariaTestLeaderContext != null) {
        reports.put(leader.id(), talariaTestLeaderContext.getAllReports());
      }
    }
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    inMemoryLeaders.get(taskId).stopGracefully();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Map<String, org.apache.druid.client.indexing.TaskStatus>> taskStatuses(Set<String> taskIds)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public OverlordServiceClient withRetryPolicy(RetryPolicy retryPolicy)
  {
    return this;
  }

  // hooks to pull stuff out for testing
  @Nullable
  Map<String, TaskReport> getReportForTask(String id)
  {
    return reports.get(id);
  }

  @Nullable
  TalariaQuerySpec getQuerySpecForTask(String id)
  {
    return talariaQuerySpec.get(id);
  }

}
