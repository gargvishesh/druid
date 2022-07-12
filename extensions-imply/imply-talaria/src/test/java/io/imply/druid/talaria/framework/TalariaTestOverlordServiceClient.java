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
import io.imply.druid.talaria.indexing.MSQControllerTask;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class TalariaTestOverlordServiceClient extends NoopOverlordClient
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
    LeaderImpl leader = null;
    TalariaTestLeaderContext talariaTestLeaderContext = null;
    try {
      talariaTestLeaderContext = new TalariaTestLeaderContext(objectMapper, injector, taskActionClient);

      MSQControllerTask cTask = objectMapper.convertValue(taskObject, MSQControllerTask.class);
      talariaQuerySpec.put(cTask.getId(), cTask.getQuerySpec());

      leader = new LeaderImpl(
          cTask,
          talariaTestLeaderContext
      );

      inMemoryLeaders.put(cTask.getId(), leader);

      leader.run();
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "Unable to run");
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
