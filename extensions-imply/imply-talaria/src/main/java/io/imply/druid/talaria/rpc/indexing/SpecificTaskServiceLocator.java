/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc.indexing;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.rpc.ServiceLocation;
import io.imply.druid.talaria.rpc.ServiceLocations;
import io.imply.druid.talaria.rpc.ServiceLocator;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.util.Collections;

public class SpecificTaskServiceLocator implements ServiceLocator
{
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final long LOCATION_CACHE_MS = 30_000;

  private final String taskId;
  private final OverlordServiceClient overlordClient;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private TaskState lastKnownState = TaskState.RUNNING; // Assume task starts out running.

  @GuardedBy("lock")
  private ServiceLocation lastKnownLocation;

  @GuardedBy("lock")
  private boolean closed = false;

  @GuardedBy("lock")
  private long lastUpdateTime = -1;

  @GuardedBy("lock")
  private SettableFuture<ServiceLocations> pendingFuture = null;

  public SpecificTaskServiceLocator(final String taskId, final OverlordServiceClient overlordClient)
  {
    this.taskId = taskId;
    this.overlordClient = overlordClient;
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    synchronized (lock) {
      if (pendingFuture != null) {
        return Futures.nonCancellationPropagating(pendingFuture);
      } else if (closed || lastKnownState != TaskState.RUNNING) {
        return Futures.immediateFuture(ServiceLocations.closed());
      } else if (lastKnownLocation == null || lastUpdateTime + LOCATION_CACHE_MS < System.currentTimeMillis()) {
        final ListenableFuture<TaskStatusResponse> taskStatusFuture;

        try {
          taskStatusFuture = overlordClient.taskStatus(taskId);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Use shared future for concurrent calls to "locate"; don't want multiple calls out to the Overlord at once.
        pendingFuture = SettableFuture.create();
        pendingFuture.addListener(
            () -> {
              if (!taskStatusFuture.isDone()) {
                // pendingFuture may resolve without taskStatusFuture due to close().
                taskStatusFuture.cancel(true);
              }
            },
            Execs.directExecutor()
        );

        Futures.addCallback(
            taskStatusFuture,
            new FutureCallback<TaskStatusResponse>()
            {
              @Override
              public void onSuccess(final TaskStatusResponse taskStatus)
              {
                synchronized (lock) {
                  if (pendingFuture != null) {
                    lastUpdateTime = System.currentTimeMillis();

                    final TaskStatusPlus statusPlus = taskStatus.getStatus();

                    if (statusPlus == null) {
                      // If the task status is unknown, we'll treat it as closed.
                      lastKnownState = null;
                      lastKnownLocation = null;
                    } else {
                      lastKnownState = statusPlus.getStatusCode();

                      if (TaskLocation.unknown().equals(statusPlus.getLocation())) {
                        lastKnownLocation = null;
                      } else {
                        lastKnownLocation = new ServiceLocation(
                            statusPlus.getLocation().getHost(),
                            statusPlus.getLocation().getPort(),
                            statusPlus.getLocation().getTlsPort(),
                            StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                        );
                      }
                    }

                    if (lastKnownState != TaskState.RUNNING) {
                      pendingFuture.set(ServiceLocations.closed());
                    } else if (lastKnownLocation == null) {
                      pendingFuture.set(ServiceLocations.forLocations(Collections.emptySet()));
                    } else {
                      pendingFuture.set(ServiceLocations.forLocation(lastKnownLocation));
                    }
                  }
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                synchronized (lock) {
                  if (pendingFuture != null) {
                    pendingFuture.setException(t);
                  }
                }
              }
            }
        );

        return Futures.nonCancellationPropagating(pendingFuture);
      } else {
        return Futures.immediateFuture(ServiceLocations.forLocation(lastKnownLocation));
      }
    }
  }

  @Override
  public void close()
  {
    synchronized (lock) {
      // Idempotent: can call close() multiple times so long as start() has already been called.
      if (!closed) {
        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.closed());
        }

        closed = true;
      }
    }
  }
}
