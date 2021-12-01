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
import com.google.common.base.Preconditions;
import com.google.common.io.CountingOutputStream;
import io.imply.druid.sql.async.AsyncQueryPoolConfig;
import io.imply.druid.sql.async.SqlAsyncLifecycleManager;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.guice.ManageLifecycleAnnouncements;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.initialization.jetty.BadRequestException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;

@ManageLifecycleAnnouncements
public class SqlAsyncQueryPool
{
  private static final EmittingLogger LOG = new EmittingLogger(SqlAsyncQueryPool.class);

  private final String brokerId;
  private final AsyncQueryPoolConfig asyncQueryPoolConfig;
  private final ThreadPoolExecutor exec;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final ObjectMapper jsonMapper;
  private final SqlAsyncLifecycleManager sqlAsyncLifecycleManager;

  public SqlAsyncQueryPool(
      final String brokerId,
      final AsyncQueryPoolConfig asyncQueryPoolConfig,
      final ThreadPoolExecutor exec,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
      final ObjectMapper jsonMapper
  )
  {
    this.brokerId = brokerId;
    this.asyncQueryPoolConfig = asyncQueryPoolConfig;
    this.exec = exec;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.jsonMapper = jsonMapper;
    this.sqlAsyncLifecycleManager = sqlAsyncLifecycleManager;
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    // Closeables are executed in LIFO order.
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
              final boolean updated = metadataManager.updateQueryDetails(
                  details.get().toError(
                      new QueryInterruptedException(
                          QueryInterruptedException.QUERY_INTERRUPTED,
                          "Interrupted by broker shutdown",
                          null,
                          null
                      )
                  )
              );
              if (!updated) {
                LOG.warn(
                    "Failed to mark query [%s] FAILED because it was already in a final state",
                    details.get().getAsyncResultId()
                );
              }
            }
          }
        }
        catch (Exception e) {
          LOG.warn(e, "Failed to update state while stopping SqlAsyncQueryPool for asyncResultIds[%s]", asyncResultId);
        }
      }
    });
    closer.register(exec::shutdownNow);
    closer.close();
  }

  public SqlAsyncQueryDetails execute(
      final String asyncResultId,
      final SqlQuery sqlQuery,
      final SqlLifecycle lifecycle,
      final String remoteAddr
  ) throws AsyncQueryAlreadyExistsException
  {
    // TODO(gianm): Document precondition: lifecycle must be in AUTHORIZED state. Validate, too?
    assert lifecycle.getAuthenticationResult() != null;

    final long timeout = getTimeout(sqlQuery.getContext());
    if (!hasTimeout(timeout)) {
      throw new BadRequestException("Query must have timeout");
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

              if (!metadataManager.updateQueryDetails(queryDetails.toRunning())) {
                throw new ISE(
                    "Failed to update query state to [%s] for [%s]",
                    queryDetails.toRunning().getState(),
                    asyncResultId
                );
              }

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
                  writer.writeHeader(
                      rowTransformer.getRowType(),
                      sqlQuery.includeTypesHeader(),
                      sqlQuery.includeSqlTypesHeader()
                  );
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

              final SqlAsyncQueryDetails complete = queryDetails.toComplete(outputStream.getCount());
              if (!metadataManager.updateQueryDetails(complete)) {
                throw new ISE(
                    "Failed to update query state to [%s] for [%s]",
                    complete.getState(),
                    asyncResultId
                );
              }
              lifecycle.finalizeStateAndEmitLogsAndMetrics(null, remoteAddr, outputStream.getCount());
            }
            catch (Exception e) {
              LOG.warn(e, "Failed to execute async query [%s]", asyncResultId);
              lifecycle.finalizeStateAndEmitLogsAndMetrics(e, remoteAddr, -1);

              try {
                final SqlAsyncQueryDetails error = queryDetails.toError(e);
                if (!metadataManager.updateQueryDetails(error)) {
                  throw new ISE(
                      "Failed to update query state to [%s] for [%s]",
                      error.getState(),
                      asyncResultId
                  );
                }
              }
              catch (Exception e2) {
                LOG.warn(e2, "Failed to set error for async query [%s]", asyncResultId);
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
      LOG.makeAlert(e, "Async query queue capacity exceeded").addData("asyncResultId", asyncResultId).emit();
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

  // These methods are copied from QueryContexts.

  private static long getDefaultTimeout(Map<String, Object> context)
  {
    final long defaultTimeout = parseLong(
        context,
        QueryContexts.DEFAULT_TIMEOUT_KEY,
        QueryContexts.DEFAULT_TIMEOUT_MILLIS
    );
    Preconditions.checkState(defaultTimeout >= 0, "Timeout must be a non negative value, but was [%s]", defaultTimeout);
    return defaultTimeout;
  }

  private static long getTimeout(Map<String, Object> context)
  {
    return parseLong(context, QueryContexts.TIMEOUT_KEY, getDefaultTimeout(context));
  }

  private static boolean hasTimeout(long timeout)
  {
    return timeout != QueryContexts.NO_TIMEOUT;
  }

  private static long parseLong(Map<String, Object> context, String key, long defaultValue)
  {
    final Object val = context.get(key);
    return val == null ? defaultValue : Numbers.parseLong(val);
  }
}
