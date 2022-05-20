/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.exec.WorkerManagerClient;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import io.imply.druid.talaria.util.FutureUtils;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;

import java.util.Map;
import java.util.Set;

/**
 * Worker manager client backed by the Indexer service. Glues together
 * three different mechanisms to provide the single Talaria interface.
 */
public class IndexerWorkerManagerClient implements WorkerManagerClient
{
  private final OverlordServiceClient overlordClient;

  public IndexerWorkerManagerClient(final OverlordServiceClient overlordClient)
  {
    this.overlordClient = overlordClient;
  }

  @Override
  public String run(String taskId, TalariaWorkerTask task)
  {
    FutureUtils.getUnchecked(overlordClient.runTask(taskId, task), true);
    return taskId;
  }

  @Override
  public void cancel(String taskId)
  {
    FutureUtils.getUnchecked(overlordClient.cancelTask(taskId), true);
  }

  @Override
  public Map<String, TaskStatus> statuses(Set<String> taskIds)
  {
    return FutureUtils.getUnchecked(overlordClient.taskStatuses(taskIds), true);
  }

  @Override
  public TaskLocation location(String id)
  {
    final TaskStatusResponse response = FutureUtils.getUnchecked(overlordClient.taskStatus(id), true);

    if (response.getStatus() != null) {
      return response.getStatus().getLocation();
    } else {
      return TaskLocation.unknown();
    }
  }

  @Override
  public void close()
  {
    // Nothing to do. The OverlordServiceClient is closed by the JVM lifecycle.
  }
}
