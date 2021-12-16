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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;

import java.util.Collection;

@JsonTypeName(KillAsyncQueryResultWithoutMetadata.JSON_TYPE_NAME)
public class KillAsyncQueryResultWithoutMetadata implements CoordinatorCustomDuty
{
  public static final String JSON_TYPE_NAME = "killAsyncQueryResultWithoutMetadata";
  static final String RESULT_REMOVED_SUCCEED_COUNT_STAT_KEY = "resultRemovedSucceedCount";
  static final String RESULT_REMOVED_FAILED_COUNT_STAT_KEY = "resultRemovedFailedCount";
  static final String RESULT_REMOVED_SUCCEED_SIZE_STAT_KEY = "resultRemovedSucceedSize";
  static final String RESULT_REMOVED_FAILED_SIZE_STAT_KEY = "resultRemovedFailedSize";
  private static final Logger log = new Logger(KillAsyncQueryResultWithoutMetadata.class);

  private final SqlAsyncResultManager sqlAsyncResultManager;
  private final SqlAsyncMetadataManager sqlAsyncMetadataManager;

  @JsonCreator
  public KillAsyncQueryResultWithoutMetadata(
      @JacksonInject SqlAsyncResultManager sqlAsyncResultManager,
      @JacksonInject SqlAsyncMetadataManager sqlAsyncMetadataManager
  )
  {
    this.sqlAsyncResultManager = sqlAsyncResultManager;
    this.sqlAsyncMetadataManager = sqlAsyncMetadataManager;
    log.info("Coordinator %s scheduling enabled", JSON_TYPE_NAME);
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    int removedCount = 0;
    int failedCount = 0;
    long removedSize = 0;
    long failedSize = 0;
    Collection<String> asyncResultIdsFromMetadata;
    Collection<String> asyncResultsFromStorage;
    try {
      asyncResultIdsFromMetadata = sqlAsyncMetadataManager.getAllAsyncResultIds();
      asyncResultsFromStorage = sqlAsyncResultManager.getAllAsyncResultIds();
    }
    catch (Exception e) {
      log.warn(e, "Failed to get async results. Skipping duty run.");
      return params;
    }

    for (String asyncResultFromStorage : asyncResultsFromStorage) {
      if (!asyncResultIdsFromMetadata.contains(asyncResultFromStorage)) {
        long size = 0;
        try {
          size = sqlAsyncResultManager.getResultSize(asyncResultFromStorage);
        }
        catch (Exception e) {
          log.debug("Failed to get file size for asyncResultId [%s]", asyncResultFromStorage);
        }
        try {
          boolean resultDeleted = sqlAsyncResultManager.deleteResults(asyncResultFromStorage);
          if (resultDeleted) {
            removedCount++;
            removedSize += size;
          } else {
            log.warn("Failed to cleanup async result for asyncResultId [%s]", asyncResultFromStorage);
            failedCount++;
            failedSize += size;
          }
        }
        catch (Exception e) {
          log.warn(e, "Failed to cleanup async result for asyncResultId [%s]", asyncResultFromStorage);
          failedCount++;
          failedSize += size;
        }
      }
    }
    log.info(
        "Finished %s duty. Removed [%,d] files with total size [%,d]. Failed to remove [%,d] files with total size [%,d].",
        JSON_TYPE_NAME,
        removedCount,
        removedSize,
        failedCount,
        failedSize
    );
    CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(RESULT_REMOVED_SUCCEED_COUNT_STAT_KEY, removedCount);
    stats.addToGlobalStat(RESULT_REMOVED_FAILED_COUNT_STAT_KEY, failedCount);
    stats.addToGlobalStat(RESULT_REMOVED_SUCCEED_SIZE_STAT_KEY, removedSize);
    stats.addToGlobalStat(RESULT_REMOVED_FAILED_SIZE_STAT_KEY, failedSize);

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

}
