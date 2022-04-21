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
import io.imply.druid.talaria.exec.LeaderStatusClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.task.ClientBasedTaskInfoProvider;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link LeaderStatusClient} for Indexer based implementation
 */
public class IndexerLeaderStatusClient implements LeaderStatusClient
{

  private final TaskInfoProvider taskInfoProvider;
  private final String leaderId;

  public IndexerLeaderStatusClient(
      @Nonnull IndexingServiceClient indexingServiceClient,
      @Nonnull String leaderId
  )
  {
    this.taskInfoProvider = new ClientBasedTaskInfoProvider(indexingServiceClient);
    this.leaderId = leaderId;
  }

  @Override
  public Optional<TaskStatus> status()
  {
    return taskInfoProvider.getTaskStatus(leaderId);
  }

  @Override
  public void close()
  {

  }
}
