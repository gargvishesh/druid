/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.sql.async.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.SqlAsyncQueryDetailsAndMetadata;
import io.imply.druid.sql.async.SqlAsyncQueryMetadata;
import io.imply.druid.sql.async.coordinator.SqlAsyncCleanupModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Optional;

@JsonTypeName(KillAsyncQueryMetadata.JSON_TYPE_NAME)
public class KillAsyncQueryMetadata implements CoordinatorCustomDuty
{
  public static final String JSON_TYPE_NAME = "killAsyncQueryMetadata";
  public static final String TIME_TO_RETAIN_KEY = "timeToRetain";
  static final String METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY = "metadataRemovedSucceedCount";
  static final String METADATA_REMOVED_FAILED_COUNT_STAT_KEY = "metadataRemovedFailedCount";
  static final String METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY = "metadataRemovedSkippedCount";
  private static final Logger log = new Logger(KillAsyncQueryMetadata.class);

  private final Duration timeToRetain;
  private final SqlAsyncMetadataManager sqlAsyncMetadataManager;

  @JsonCreator
  public KillAsyncQueryMetadata(
      @JsonProperty(KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY) Duration timeToRetain,
      @JacksonInject SqlAsyncMetadataManager sqlAsyncMetadataManager
  )
  {
    this.timeToRetain = timeToRetain;
    Preconditions.checkArgument(
        this.timeToRetain != null && this.timeToRetain.getMillis() >= 0,
        StringUtils.format(
            "Coordinator async result cleanup duty timeToRetain [%s] must be >= 0",
            SqlAsyncCleanupModule.CLEANUP_TIME_TO_RETAIN_CONFIG_KEY
        )
    );
    this.sqlAsyncMetadataManager = sqlAsyncMetadataManager;
    log.info(
        "Coordinator %s scheduling enabled with timeToRetain [%s]",
        JSON_TYPE_NAME,
        this.timeToRetain
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    Collection<String> asyncResultIds;
    try {
      asyncResultIds = sqlAsyncMetadataManager.getAllAsyncResultIds();
    }
    catch (Exception e) {
      log.warn(e, "Failed to get asyncResultIds to clean up. Skipping async result cleanup duty run.");
      return params;
    }
    int removed = 0;
    int skipped = 0;
    int failed = 0;
    for (String asyncResultId : asyncResultIds) {
      try {
        Optional<SqlAsyncQueryDetailsAndMetadata> queryDetailsAndStat = sqlAsyncMetadataManager.getQueryDetailsAndMetadata(asyncResultId);
        if (!queryDetailsAndStat.isPresent()) {
          log.warn("Failed to get details for asyncResultId [%s]", asyncResultId);
          failed++;
          continue;
        }
        SqlAsyncQueryDetails sqlAsyncQueryDetails = queryDetailsAndStat.get().getSqlAsyncQueryDetails();
        SqlAsyncQueryMetadata metadata = queryDetailsAndStat.get().getMetadata();
        if (metadata == null) {
          log.warn("Failed to get updated timestamp for asyncResultId [%s]", asyncResultId);
          failed++;
          continue;
        }
        if (isQueryEligibleForCleanup(sqlAsyncQueryDetails, metadata.getLastUpdatedTime(), timeToRetain.getMillis())) {
          boolean metadataDeleted = sqlAsyncMetadataManager.removeQueryDetails(sqlAsyncQueryDetails);
          if (!metadataDeleted) {
            log.warn("Failed to cleanup async metadata for asyncResultId [%s]", asyncResultIds);
            failed++;
          } else {
            removed++;
          }
        } else {
          skipped++;
        }
      }
      catch (Exception e) {
        log.warn(e, "Failed to cleanup async metadata for asyncResultId [%s]", asyncResultIds);
        failed++;
      }
    }
    log.info("Finished %s duty. Removed [%,d]. Failed [%,d]. Skipped [%,d].", JSON_TYPE_NAME, removed, failed, skipped);
    CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY, removed);
    stats.addToGlobalStat(METADATA_REMOVED_FAILED_COUNT_STAT_KEY, failed);
    stats.addToGlobalStat(METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY, skipped);
    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  @VisibleForTesting
  static boolean isQueryEligibleForCleanup(SqlAsyncQueryDetails queryDetails, long updatedTime, long retainDuration)
  {
    return ((queryDetails.getState() == SqlAsyncQueryDetails.State.COMPLETE
         || queryDetails.getState() == SqlAsyncQueryDetails.State.FAILED
         || queryDetails.getState() == SqlAsyncQueryDetails.State.UNDETERMINED)
        && updatedTime <= (System.currentTimeMillis() - retainDuration));
  }
}
