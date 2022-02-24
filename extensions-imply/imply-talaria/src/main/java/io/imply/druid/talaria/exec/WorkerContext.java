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
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.kernel.QueryDefinition;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.server.DruidNode;

import java.io.File;

public interface WorkerContext
{
  ObjectMapper jsonMapper();
  // TODO(paul): Per Gian, this is a hack in TalariaWorkerTask carried over here.
  Injector injector();

  /**
   * Callback from the worker implementation to "register" the worker. Used in
   * the indexer to set up the task chat services. Does nothing in the Talaria
   * server.
   */
  void registerWorker(Worker worker, Closer closer);

  LeaderClient makeLeaderClient(String leaderId);
  WorkerClient makeWorkerClient(String workerId);
  // TODO(paul): temporary, remove once clients are distributed
  TalariaTaskClient makeTaskClient();
  File tempDir();
  FrameContext frameContext(QueryDefinition queryDef, int stageNumber);
  int threadCount();
  DruidNode selfNode();
  Bouncer processorBouncer();
}
