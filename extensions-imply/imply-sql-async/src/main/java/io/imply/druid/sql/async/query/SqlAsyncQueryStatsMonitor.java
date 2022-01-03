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
import com.google.inject.name.Named;
import io.imply.druid.sql.async.AsyncQueryConfig;
import io.imply.druid.sql.async.SqlAsyncModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;

public class SqlAsyncQueryStatsMonitor extends AbstractMonitor
{
  private final String brokerId;
  private final SqlAsyncQueryPool sqlAsyncQueryPool;
  private final AsyncQueryConfig asyncQueryLimitsConfig;

  @Inject
  public SqlAsyncQueryStatsMonitor(
      @Named(SqlAsyncModule.ASYNC_BROKER_ID) final String brokerId,
      SqlAsyncQueryPool sqlAsyncQueryPool,
      AsyncQueryConfig asyncQueryLimitsConfig
  )
  {
    this.brokerId = brokerId;
    this.sqlAsyncQueryPool = sqlAsyncQueryPool;
    this.asyncQueryLimitsConfig = asyncQueryLimitsConfig;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    SqlAsyncQueryPool.BestEffortStatsSnapshot sqlAsyncQueryPoolStats = sqlAsyncQueryPool.getBestEffortStatsSnapshot();
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.SERVER, brokerId)
            .build("async/sqlQuery/running/count", sqlAsyncQueryPoolStats.getQueryRunningCount())
    );
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.SERVER, brokerId)
            .build("async/sqlQuery/queued/count", sqlAsyncQueryPoolStats.getQueryQueuedCount())
    );

    // Emit stats from limit configs
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.SERVER, brokerId)
            .build("async/sqlQuery/running/max", asyncQueryLimitsConfig.getMaxConcurrentQueries())
    );
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.SERVER, brokerId)
            .build("async/sqlQuery/queued/max", asyncQueryLimitsConfig.getMaxQueriesToQueue())
    );

    return true;
  }
}
