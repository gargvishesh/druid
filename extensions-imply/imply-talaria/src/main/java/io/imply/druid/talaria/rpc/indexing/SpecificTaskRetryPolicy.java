/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.indexing;

import io.imply.druid.talaria.rpc.RetryPolicy;
import io.imply.druid.talaria.rpc.StandardRetryPolicy;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Retry policy for tasks. Meant to be used together with {@link SpecificTaskServiceLocator}.
 *
 * Uses unlimited attempts, because it makes sense to keep retrying as long as the task locator says the task may
 * come back. In essence, we're relying on the Overlord's task-monitoring mechanism to tell us when to give up.
 *
 * Returns true from {@link #retryHttpResponse} when encountering an HTTP 404 with a
 * {@link ChatHandlerResource#TASK_ID_HEADER} header for a different task. This can happen when a task is suspended and
 * then later restored in a different location, and then some *other* task reuses its old port. This task-mismatch
 * scenario is retried indefinitely, since we expect that the {@link SpecificTaskServiceLocator} will update the
 * location at some point.
 */
public class SpecificTaskRetryPolicy implements RetryPolicy
{
  private final String taskId;

  public SpecificTaskRetryPolicy(final String taskId)
  {
    this.taskId = taskId;
  }

  @Override
  public int maxAttempts()
  {
    // Unlimited retries, which means we'll keep retrying as long as the task is still running.
    // (When the task stops running, the SpecificTaskServiceLocator will return an empty set and retries will stop.)
    return UNLIMITED;
  }

  @Override
  public boolean retryHttpResponse(HttpResponse response)
  {
    return StandardRetryPolicy.unlimited().retryHttpResponse(response) || isTaskMismatch(response);
  }

  @Override
  public boolean retryThrowable(Throwable t)
  {
    return StandardRetryPolicy.unlimited().retryThrowable(t);
  }

  private boolean isTaskMismatch(final HttpResponse response)
  {
    // See class-level javadocs for details on why we do this.
    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      final String headerTaskId = StringUtils.urlDecode(response.headers().get(ChatHandlerResource.TASK_ID_HEADER));
      return headerTaskId != null && !headerTaskId.equals(taskId);
    } else {
      return false;
    }
  }
}
