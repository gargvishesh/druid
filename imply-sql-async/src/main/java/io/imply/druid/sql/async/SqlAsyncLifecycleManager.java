/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@LazySingleton
public class SqlAsyncLifecycleManager
{
  private final SqlLifecycleManager sqlLifecycleManager;


  //TODO: add a pojo so that we can easily add child objects;
  private final ConcurrentHashMap<String, Future<?>> asyncSubmitFutures = new ConcurrentHashMap<>();

  @Inject
  public SqlAsyncLifecycleManager(SqlLifecycleManager sqlLifecycleManager)
  {
    this.sqlLifecycleManager = sqlLifecycleManager;
  }


  public void add(String asyncResultId, SqlLifecycle lifecycle, Future<?> submitFuture)
  {
    sqlLifecycleManager.add(asyncResultId, lifecycle);
    asyncSubmitFutures.put(asyncResultId, submitFuture);
  }

  /**
   * For the given asyncResultId, this method removes all lifecycles
   */
  public void remove(String asyncResultId)
  {
    sqlLifecycleManager.removeAll(asyncResultId, sqlLifecycleManager.getAll(asyncResultId));
    asyncSubmitFutures.remove(asyncResultId);
  }

  /**
   * Cancel the query
   *
   * @param asyncResultId asyncResultId
   */
  public void cancel(String asyncResultId)
  {
    sqlLifecycleManager.getAll(asyncResultId).forEach(SqlLifecycle::cancel);
    Future<?> submitFuture = asyncSubmitFutures.get(asyncResultId);
    if (submitFuture != null) {
      asyncSubmitFutures.remove(asyncResultId);
      submitFuture.cancel(true);
    }
  }
}
