/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.query;

import com.google.inject.Inject;
import io.imply.druid.sql.async.AsyncQueryLimitsConfig;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.Collection;

public class SqlAsyncQueryStatsMonitor extends AbstractMonitor
{
  private static final Logger LOG = new Logger(SqlAsyncQueryStatsMonitor.class);

  private final SqlAsyncMetadataManager metadataManager;
  private final SqlAsyncQueryPool sqlAsyncQueryPool;
  private final AsyncQueryLimitsConfig asyncQueryLimitsConfig;

  @Inject
  public SqlAsyncQueryStatsMonitor(
      SqlAsyncMetadataManager metadataManager,
      SqlAsyncQueryPool sqlAsyncQueryPool,
      AsyncQueryLimitsConfig asyncQueryLimitsConfig
  )
  {
    this.metadataManager = metadataManager;
    this.sqlAsyncQueryPool = sqlAsyncQueryPool;
    this.asyncQueryLimitsConfig = asyncQueryLimitsConfig;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    // Emit stats from current state
    try {
      Collection<String> allAsyncResultIds = metadataManager.getAllAsyncResultIds();

      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/result/tracked/count",
              allAsyncResultIds.size()
          )
      );

      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/result/tracked/bytes",
              metadataManager.totalCompleteQueryResultsSize(allAsyncResultIds)
          )
      );
    }
    catch (Exception e) {
      LOG.warn(e, "Cannot emit metrics as fail get all async result ids from metadata");
    }

    SqlAsyncQueryPool.BestEffortStatsSnapshot sqlAsyncQueryPoolStats = sqlAsyncQueryPool.getBestEffortStatsSnapshot();
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "async/sqlQuery/running/count",
            sqlAsyncQueryPoolStats.getQueryRunningCount()
        )
    );
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "async/sqlQuery/queued/count",
            sqlAsyncQueryPoolStats.getQueryQueuedCount()
        )
    );

    // Emit stats from limit configs
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "async/sqlQuery/tracked/max",
            asyncQueryLimitsConfig.getMaxAsyncQueries()
        )
    );
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "async/sqlQuery/running/max",
            asyncQueryLimitsConfig.getMaxConcurrentQueries()
        )
    );
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "async/sqlQuery/queued/max",
            asyncQueryLimitsConfig.getMaxQueriesToQueue()
        )
    );
    return true;
  }
}
