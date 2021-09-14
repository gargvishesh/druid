/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Segment;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * This class defers the actual execution and creation of query runner to the time after segment has been downloaded.
 * Query runner creation is deferred too so that {@link org.apache.druid.query.QueryRunnerFactory#createRunner(Segment)}
 * can access physical properties of segment if required.
 *
 * If the query for this runner gets cancelled, {@link #tearDown()} must be
 * called explicitly so that the resources held by this runner are released.
 */
public class DeferredQueryRunner<T> implements QueryRunner<T>
{
  private final ListenableFuture<Void> downloadFuture;
  private final Segment segment;
  private final Closeable resource;
  private final Supplier<QueryRunner<T>> baseRunnerSupplier;

  public DeferredQueryRunner(
      final ListenableFuture<Void> downloadFuture,
      final Segment segment,
      final Closeable resource,
      final Supplier<QueryRunner<T>> baseRunnerSupplier
  )
  {
    this.downloadFuture = downloadFuture;
    this.segment = segment;
    this.resource = resource;
    this.baseRunnerSupplier = baseRunnerSupplier;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    try {
      if (downloadFuture.isDone()) {
        try {
          // Download should be completed
          downloadFuture.get();
          return baseRunnerSupplier.get().run(queryPlus, responseContext);
        }
        catch (Exception e) {
          throw new IAE(
              e,
              "Future is done but the download was not completed sucessfull for segment [%s]",
              segment.getId()
          );
        }
      }
      throw new IAE("Run shouldn't be called till segment [%s] is downloaed", segment.getId());
    }
    finally {
      tearDown();
    }
  }

  public ListenableFuture<Void> getDownloadFuture()
  {
    return downloadFuture;
  }

  /**
   * Releases resources that this Query Runner has held. This method should be
   * called explicitly only when the query is cancelled.
   */
  public void tearDown()
  {
    try {
      resource.close();
    }
    catch (IOException ioe) {
      //TODO: log
    }
  }

}
