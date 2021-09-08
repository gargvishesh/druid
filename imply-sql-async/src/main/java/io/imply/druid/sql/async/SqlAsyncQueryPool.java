/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CountingOutputStream;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

public class SqlAsyncQueryPool
{
  private static final Logger log = new Logger(SqlAsyncQueryPool.class);

  private final ExecutorService exec;
  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncResultManager resultManager;
  private final ObjectMapper jsonMapper;
  private final AsyncQueryLimitsConfig asyncQueryLimitsConfig;

  public SqlAsyncQueryPool(
      final ExecutorService exec,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      AsyncQueryLimitsConfig asyncQueryLimitsConfig,
      final ObjectMapper jsonMapper
  )
  {
    this.exec = exec;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.jsonMapper = jsonMapper;
    this.asyncQueryLimitsConfig = asyncQueryLimitsConfig;
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
    int currentRetainQueryCount = metadataManager.getAllAsyncResultIds().size();
    if (currentRetainQueryCount >= asyncQueryLimitsConfig.getMaxAsyncQueries()) {
      String errorMessage = StringUtils.format(
          "Too many retained queries, total query retained of %s exceeded. Please try your query again later.",
          asyncQueryLimitsConfig.getMaxAsyncQueries()
      );
      throw new QueryCapacityExceededException(
          QueryCapacityExceededException.ERROR_CODE,
          errorMessage,
          QueryCapacityExceededException.class.getName(),
          null
      );
    }

    final SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew(
        asyncResultId,
        lifecycle.getAuthenticationResult().getIdentity(),
        sqlQuery.getResultFormat()
    );

    metadataManager.addNewQuery(queryDetails);
    try {
      exec.submit(
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
            }
          }
      );
    }
    catch (QueryCapacityExceededException e) {
      metadataManager.removeQueryDetails(queryDetails);
      throw e;
    }

    return queryDetails;
  }

  @VisibleForTesting
  public void shutdownNow()
  {
    exec.shutdownNow();
  }
}
