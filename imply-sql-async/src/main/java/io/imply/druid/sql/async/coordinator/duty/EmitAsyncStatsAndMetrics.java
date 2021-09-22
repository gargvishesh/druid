/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;

import java.util.Collection;

@JsonTypeName("emitAsyncStatsAndMetrics")
public class EmitAsyncStatsAndMetrics implements CoordinatorCustomDuty
{
  private static final Logger LOG = new Logger(EmitAsyncStatsAndMetrics.class);

  private final SqlAsyncMetadataManager metadataManager;

  public EmitAsyncStatsAndMetrics(
      SqlAsyncMetadataManager metadataManager
  )
  {
    this.metadataManager = metadataManager;
    LOG.info("Coordinator emitAsyncStatsAndMetrics scheduling enabled");
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    try {
      CoordinatorStats stats = params.getCoordinatorStats();
      ServiceEmitter emitter = params.getEmitter();

      // Emit stats from cleanup duties
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/result/removed/count",
              stats.getGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_SUCCEED_COUNT_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/result/failed/count",
              stats.getGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_FAILED_COUNT_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/result/removed/bytes",
              stats.getGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_SUCCEED_SIZE_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/result/failed/bytes",
              stats.getGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_FAILED_SIZE_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/metadata/removed/count",
              stats.getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/metadata/failed/count",
              stats.getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_FAILED_COUNT_STAT_KEY)
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "async/cleanup/metadata/skipped/count",
              stats.getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY)
          )
      );

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

      // Emit coordinator runtime stats
      emitDutyStats(emitter, "coordinator/time", stats, "runtime");
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to emit async stats and metrics.");
      return params;
    }
    return params;
  }

  private void emitDutyStats(
      final ServiceEmitter emitter,
      final String metricName,
      final CoordinatorStats stats,
      final String statName
  )
  {
    stats.forEachDutyStat(
        statName,
        (final String duty, final long count) -> {
          emitDutyStat(emitter, metricName, duty, count);
        }
    );
  }

  private void emitDutyStat(
      final ServiceEmitter emitter,
      final String metricName,
      final String duty,
      final long value
  )
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY, duty)
            .build(metricName, value)
    );
  }
}
