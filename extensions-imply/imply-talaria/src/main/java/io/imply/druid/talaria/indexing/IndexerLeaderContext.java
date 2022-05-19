/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.WorkerClient;
import io.imply.druid.talaria.exec.WorkerManagerClient;
import io.imply.druid.talaria.rpc.DruidServiceClientFactory;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.server.DruidNode;

import java.util.Map;

/**
 * Indexer implementation of the Talaria leader context.
 */
public class IndexerLeaderContext implements LeaderContext
{
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final DruidServiceClientFactory clientFactory;
  private final OverlordServiceClient overlordClient;
  private final WorkerManagerClient workerManager;

  public IndexerLeaderContext(
      final TaskToolbox toolbox,
      final Injector injector,
      final DruidServiceClientFactory clientFactory,
      final OverlordServiceClient overlordClient
  )
  {
    this.toolbox = toolbox;
    this.injector = injector;
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.workerManager = new IndexerWorkerManagerClient(overlordClient);
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public DruidNode selfNode()
  {
    return injector.getInstance(Key.get(DruidNode.class, Self.class));
  }

  @Override
  public CoordinatorClient coordinatorClient()
  {
    return toolbox.getCoordinatorClient();
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return toolbox.getTaskActionClient();
  }

  @Override
  public WorkerClient taskClientFor(Leader leader)
  {
    // Ignore leader parameter.
    return new IndexerWorkerClient(clientFactory, overlordClient, jsonMapper());
  }

  @Override
  public void registerLeader(Leader leader, final Closer closer)
  {
    ChatHandler chatHandler = new LeaderChatHandler(toolbox, leader);
    toolbox.getChatHandlerProvider().register(leader.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(leader.id()));
  }

  @Override
  public WorkerManagerClient workerManager()
  {
    return workerManager;
  }

  @Override
  public void writeReports(String taskId, Map<String, TaskReport> reports)
  {
    toolbox.getTaskReportFileWriter().write(taskId, reports);
  }
}
