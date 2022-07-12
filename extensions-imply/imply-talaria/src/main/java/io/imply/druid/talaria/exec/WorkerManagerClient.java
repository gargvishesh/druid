/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

/**
 * Generic Talaria interface to the "worker manager" mechanism which
 * starts, cancels and monitors worker tasks.
 */
public interface WorkerManagerClient extends Closeable
{
  String run(String leaderId, TalariaWorkerTask task);

  /**
   * @param workerId the task ID
   *
   * @return a {@code TaskLocation} associated with the task or
   * {@code TaskLocation.unknown()} if no associated entry could be found
   */
  // TODO(paul): Change so that workers come with their location attached.
  TaskLocation location(String workerId);

  // TODO(paul): Remove: workers should be ephemeral.
  Map<String, TaskStatus> statuses(Set<String> taskIds);

  void cancel(String workerId);

  @Override
  void close();
}
