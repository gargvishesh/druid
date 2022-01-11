/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.server.DruidNode;

import java.util.Map;

/**
 * Provides services which the Talaria leader needs to do its job. Designed
 * as an interface so the services can be implemented differently in the Indexer
 * and Talaria server.
 */
public interface LeaderContext
{
  ObjectMapper jsonMapper();
  // TODO(paul): Per Gian, this is a hack in TalariaControllerTask carried over here.
  Injector injector();
  DruidNode selfNode();

  /**
   * Provide access to the Coordinator service.
   */
  CoordinatorClient coordinatorClient();

  /**
   * Provide access to segment actions in the Overlord.
   * Despite the name: this is a client that <b>tasks</b>
   * to take Overlord <b>actions</b>.
   */
  // TODO(paul): This must be modified to allow taking actions even if the
  // task is not running within the Overlord system.
  TaskActionClient taskActionClient();

  /**
   * The "worker manager client" provides services about workers: starting,
   * canceling, obtaining status.
   */
  WorkerManagerClient workerManager();

  /**
   * Callback from the leader implementation to "register" the leader. Used in
   * the indexer to set up the task chat services. Does nothing in the Talaria
   * server.
   */
  void registerLeader(Leader leader, Closer closer);

  /**
   * The worker client interfaces to a specific worker.
   */
  WorkerClient taskClientFor(Leader leader);

  void writeReports(String taskId, Map<String, TaskReport> reports);
}
