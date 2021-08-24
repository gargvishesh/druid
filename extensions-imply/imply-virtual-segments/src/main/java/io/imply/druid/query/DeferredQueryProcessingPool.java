/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.PrioritizedQueryRunnerCallable;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;

import javax.inject.Inject;

/**
 * Links the future object corresponding to query execution for a segment, with the download future for the segment.
 * Thus, query execution is triggered when the download future completes successfully.
 */
public class DeferredQueryProcessingPool extends ForwardingListeningExecutorService implements QueryProcessingPool
{
  private final QueryProcessingPool delegate;

  @Inject
  DeferredQueryProcessingPool(QueryProcessingPool delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
  {
    QueryRunner<V> queryRunner = task.getRunner();
    if (queryRunner instanceof DeferredQueryRunner) {
      DeferredQueryRunner<?> deferredQueryRunner = (DeferredQueryRunner<?>) queryRunner;
      ListenableFuture<T> runnerFuture = Futures.transform(
          deferredQueryRunner.getDownloadFuture(),
          (AsyncFunction<Void, T>) input -> delegate.submit(task)
      );
      addCancelCallback(runnerFuture, deferredQueryRunner::tearDown);
      return runnerFuture;
    }

    throw new IAE(
        "Expected queryRunner to be of type DeferredQueryRunner, instead found [%s]",
        queryRunner.getClass()
    );
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

  /**
   * Adds a callback to be invoked if the given future is cancelled.
   */
  private <T> void addCancelCallback(ListenableFuture<T> future, Runnable callback)
  {
    Futures.addCallback(
        future,
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(T result)
          {

          }

          @Override
          public void onFailure(Throwable t)
          {
            if (future.isCancelled()) {
              callback.run();
            }
          }
        }
    );
  }
}
