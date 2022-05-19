/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.rpc.RetryPolicy;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;

import java.util.Map;
import java.util.Set;

/**
 * High-level Overlord client.
 */
public interface OverlordServiceClient
{
  ListenableFuture<Void> runTask(String taskId, Object taskObject);

  ListenableFuture<Void> cancelTask(String taskId);

  ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds);

  ListenableFuture<TaskStatusResponse> taskStatus(String taskId);

  OverlordServiceClient withRetryPolicy(RetryPolicy retryPolicy);
}
