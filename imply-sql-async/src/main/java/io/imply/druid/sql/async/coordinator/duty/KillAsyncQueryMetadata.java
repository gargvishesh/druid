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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Optional;

@JsonTypeName("killAsyncQueryMetadata")
public class KillAsyncQueryMetadata implements CoordinatorCustomDuty
{
  private static final Logger log = new Logger(KillAsyncQueryMetadata.class);

  private final Duration retainDuration;
  private final SqlAsyncMetadataManager sqlAsyncMetadataManager;

  @JsonCreator
  public KillAsyncQueryMetadata(
      @JsonProperty("retainDuration") Duration retainDuration,
      @JacksonInject SqlAsyncMetadataManager sqlAsyncMetadataManager
  )
  {
    this.retainDuration = retainDuration;
    Preconditions.checkArgument(this.retainDuration != null && this.retainDuration.getMillis() >= 0, "Coordinator async result cleanup duty retainDuration must be >= 0");
    this.sqlAsyncMetadataManager = sqlAsyncMetadataManager;
    log.info(
        "Coordinator killAsyncQueryMetadata scheduling enabled with retainDuration [%s]",
        this.retainDuration
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
        if (isQueryEligibleForCleanup(sqlAsyncQueryDetails, metadata.getLastUpdatedTime(), retainDuration.getMillis())) {
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
    log.info("Finished killAsyncQueryMetadata duty. Removed [%,d]. Failed [%,d]. Skipped [%,d].", removed, failed, skipped);
    return params;
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
