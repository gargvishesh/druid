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
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;

import java.util.Collection;

/**
 * This duty just needs to collect required stats. All stats which can be
 * emitted as metrics are emitted by the Coordinator by default.
 */
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
      // We just collect stats here and don't emit them
      final CoordinatorRunStats stats = params.getCoordinatorStats();

      final Collection<String> allAsyncResultIds = metadataManager.getAllAsyncResultIds();
      stats.add(Stats.TRACKED_RESULTS, allAsyncResultIds.size());
      stats.add(
          Stats.TRACKED_RESULT_BYTES,
          metadataManager.totalCompleteQueryResultsSize(allAsyncResultIds)
      );
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to collect async stats and metrics.");
    }
    return params;
  }
}
