/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import io.imply.druid.sql.async.AsyncQueryLimitsConfig;
import io.imply.druid.sql.async.SqlAsyncLifecycleManager;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;

public class SqlAsyncQueryPool
{
  private static final EmittingLogger log = new EmittingLogger(SqlAsyncQueryPool.class);

  private final ThreadPoolExecutor exec;
  private final String brokerId;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final ObjectMapper jsonMapper;
  private final AsyncQueryLimitsConfig asyncQueryLimitsConfig;
  private final SqlAsyncLifecycleManager sqlAsyncLifecycleManager;

  public SqlAsyncQueryPool(
      final ThreadPoolExecutor exec,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      AsyncQueryLimitsConfig asyncQueryLimitsConfig,
      final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
      final ObjectMapper jsonMapper,
      final String brokerId
  )
  {
    this.exec = exec;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.jsonMapper = jsonMapper;
    this.asyncQueryLimitsConfig = asyncQueryLimitsConfig;
    this.sqlAsyncLifecycleManager = sqlAsyncLifecycleManager;
    this.brokerId = brokerId;
  }

  public SqlAsyncQueryDetails execute(
      final String asyncResultId,
      final SqlQuery sqlQuery,
      final SqlLifecycle lifecycle,
      final String remoteAddr
  ) throws IOException, AsyncQueryAlreadyExistsException
  {
    // TODO(gianm): Document precondition: lifecycle must be in AUTHORIZED state. Validate, too?
    assert lifecycle.getAuthenticationResult() != null;
    // Check if we are under retention number of queries limit. Reject query if we are over the limit
    int currentQueryCount = metadataManager.getAllAsyncResultIds().size();
    if (currentQueryCount >= asyncQueryLimitsConfig.getMaxAsyncQueries()) {
      String errorMessage = StringUtils.format(
          "Too many async queries. Total async query limit of %s exceeded. Please try your query again later.",
          asyncQueryLimitsConfig.getMaxAsyncQueries()
      );
      QueryCapacityExceededException e = new QueryCapacityExceededException(
          QueryCapacityExceededException.ERROR_CODE,
          errorMessage,
          QueryCapacityExceededException.class.getName(),
          null
      );
      log.makeAlert(e, "Total async query limit exceeded").addData("asyncResultId", asyncResultId).emit();
      throw e;
    }

    final SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew(
        asyncResultId,
        lifecycle.getAuthenticationResult().getIdentity(),
        sqlQuery.getResultFormat()
    );

    metadataManager.addNewQuery(queryDetails);

    try {
      sqlAsyncLifecycleManager.add(asyncResultId, lifecycle, exec.submit(
          () -> {
            final String currThreadName = Thread.currentThread().getName();

            try {
              Thread.currentThread().setName(StringUtils.format("sql-async[%s]", asyncResultId));

              metadataManager.updateQueryDetails(queryDetails.toRunning());

              // TODO(gianm): Most of this code is copy-pasted from SqlResource
              lifecycle.plan();
              SqlRowTransformer rowTransformer = lifecycle.createRowTransformer();
              Yielder<Object[]> yielder = Yielders.each(lifecycle.execute());

              CountingOutputStream outputStream;

              try (
                  final OutputStream baseOutputStream = resultManager.writeResults(queryDetails);
                  final ResultFormat.Writer writer =
                      sqlQuery.getResultFormat()
                              .createFormatter((outputStream = new CountingOutputStream(baseOutputStream)), jsonMapper)
              ) {
                writer.writeResponseStart();

                if (sqlQuery.includeHeader()) {
                  writer.writeHeader(rowTransformer.getFieldList());
                }

                while (!yielder.isDone()) {
                  final Object[] row = yielder.get();
                  writer.writeRowStart();
                  for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
                    final Object value = rowTransformer.transform(row, i);
                    writer.writeRowField(rowTransformer.getFieldList().get(i), value);
                  }
                  writer.writeRowEnd();
                  yielder = yielder.next(null);
                }

                writer.writeResponseEnd();
              }
              finally {
                yielder.close();
              }

              metadataManager.updateQueryDetails(queryDetails.toComplete(outputStream.getCount()));
              lifecycle.finalizeStateAndEmitLogsAndMetrics(null, remoteAddr, outputStream.getCount());
            }
            catch (Exception e) {
              log.warn(e, "Failed to execute async query [%s]", asyncResultId);
              lifecycle.finalizeStateAndEmitLogsAndMetrics(e, remoteAddr, -1);

              try {
                metadataManager.updateQueryDetails(queryDetails.toError(e));
              }
              catch (Exception e2) {
                log.warn(e2, "Failed to set error for async query [%s]", asyncResultId);
              }
            }
            finally {
              Thread.currentThread().setName(currThreadName);
              sqlAsyncLifecycleManager.remove(asyncResultId);
            }
          }
      ));
    }
    catch (QueryCapacityExceededException e) {
      // The QueryCapacityExceededException is thrown by the Executor's RejectedExecutionHandler
      // when the Executor's queue is full. The Executor's queue size is control by Druid
      // See more details in SqlAsyncQueryPoolProvider#get()
      metadataManager.removeQueryDetails(queryDetails);
      sqlAsyncLifecycleManager.remove(asyncResultId);
      log.makeAlert(e, "Async query queue capacity exceeded").addData("asyncResultId", asyncResultId).emit();
      throw e;
    }

    return queryDetails;
  }

  public BestEffortStatsSnapshot getBestEffortStatsSnapshot()
  {
    // There can be race in metrics values as metric values are retrieved sequentially
    // However, this is fine as these metrics are only use for emitting stats and does not have to be perfect
    return new BestEffortStatsSnapshot(
        exec.getActiveCount(),
        exec.getQueue().size()
    );
  }

  /**
   * This class is created by {@link SqlAsyncQueryPool#getBestEffortStatsSnapshot()} which
   * can have race in populating the metric values.
   */
  public static class BestEffortStatsSnapshot
  {
    private final int queryRunningCount;
    private final int queryQueuedCount;

    public BestEffortStatsSnapshot(
        final int queryRunningCount,
        final int queryQueuedCount
    )
    {
      this.queryRunningCount = queryRunningCount;
      this.queryQueuedCount = queryQueuedCount;
    }

    public int getQueryRunningCount()
    {
      return queryRunningCount;
    }

    public int getQueryQueuedCount()
    {
      return queryQueuedCount;
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    Closer closer = Closer.create();
    closer.register(() -> {
      Collection<String> asyncResultIds = metadataManager.getAllAsyncResultIds();
      for (String asyncResultId : asyncResultIds) {
        try {
          if (brokerId.equals(SqlAsyncUtil.getBrokerIdFromAsyncResultId(asyncResultId))) {
            Optional<SqlAsyncQueryDetails> details = metadataManager.getQueryDetails(asyncResultId);
            if (details.isPresent()
                && (details.get().getState() == SqlAsyncQueryDetails.State.INITIALIZED
                    || details.get().getState() == SqlAsyncQueryDetails.State.RUNNING
                )
            ) {
              metadataManager.updateQueryDetails(
                  details.get().toError(
                      new QueryInterruptedException(
                          QueryInterruptedException.QUERY_INTERRUPTED,
                          "Interrupted by broker shutdown",
                          null,
                          null
                      )
                  )
              );
            }
          }
        }
        catch (Exception e) {
          log.warn(e, "Failed to update state while stopping SqlAsyncQueryPool for asyncResultIds[%s]", asyncResultId);
        }
      }
    });
    closer.register(exec::shutdownNow);
    closer.close();
  }
}
