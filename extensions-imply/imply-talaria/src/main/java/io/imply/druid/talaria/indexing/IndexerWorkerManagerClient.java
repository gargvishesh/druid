/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.base.Optional;
import io.imply.druid.talaria.exec.WorkerManagerClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;

import java.util.Map;
import java.util.Set;

/**
 * Worker manager client backed by the Indexer service. Glues together
 * three different mechanisms to provide the single Talaria interface.
 */
public class IndexerWorkerManagerClient implements WorkerManagerClient
{
  private final IndexingServiceClient indexingServiceClient;
  private final TaskInfoProvider taskInfoProvider;

  public IndexerWorkerManagerClient(
      IndexingServiceClient indexingServiceClient)
  {
    this.indexingServiceClient = indexingServiceClient;
    this.taskInfoProvider = new ClientBasedTaskInfoProvider(indexingServiceClient);
  }

  @Override
  public String run(String taskId, TalariaWorkerTask task)
  {
    return indexingServiceClient.runTask(taskId, task);
  }

  @Override
  public String cancel(String taskId)
  {
    return indexingServiceClient.cancelTask(taskId);
  }

  @Override
  public Map<String, TaskStatus> statuses(Set<String> taskIds) throws InterruptedException
  {
    return indexingServiceClient.getTaskStatuses(taskIds);
  }

  @Override
  public TaskLocation location(String id)
  {
    return taskInfoProvider.getTaskLocation(id);
  }

  @Override
  public Optional<org.apache.druid.indexer.TaskStatus> status(String id)
  {
    return taskInfoProvider.getTaskStatus(id);
  }

  @Override
  public void close()
  {
    // Not a real client: don't actually close.
  }
}
