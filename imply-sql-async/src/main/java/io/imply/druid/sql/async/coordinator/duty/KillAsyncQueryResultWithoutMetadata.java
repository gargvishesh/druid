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
import io.imply.druid.sql.async.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.SqlAsyncResultManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;

import java.util.Collection;

@JsonTypeName("killAsyncQueryResultWithoutMetadata")
public class KillAsyncQueryResultWithoutMetadata implements CoordinatorCustomDuty
{
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
    log.info("Coordinator killAsyncQueryResultWithoutMetadata scheduling enabled");
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    int removed = 0;
    int failed = 0;
    Collection<String> asyncResultIdsFromMetadata;
    Collection<String> asyncResultsFromStorage;
    try {
      asyncResultIdsFromMetadata = sqlAsyncMetadataManager.getAllAsyncResultIds();
      asyncResultsFromStorage = sqlAsyncResultManager.getAllResults();
    }
    catch (Exception e) {
      log.warn(e, "Failed to get async results. Skipping duty run.");
      return params;
    }

    for (String asyncResultFromStorage : asyncResultsFromStorage) {
      try {
        if (!asyncResultIdsFromMetadata.contains(asyncResultFromStorage)) {
          boolean resultDeleted = sqlAsyncResultManager.deleteResults(asyncResultFromStorage);
          if (resultDeleted) {
            removed++;
          } else {
            log.warn("Failed to cleanup async result for asyncResultId [%s]", asyncResultFromStorage);
            failed++;
          }
        }
      }
      catch (Exception e) {
        log.warn(e, "Failed to cleanup async result for asyncResultId [%s]", asyncResultFromStorage);
        failed++;
      }
    }
    log.info("Finished killAsyncQueryResultWithoutMetadata duty. Removed [%,d]. Failed [[%,d].", removed, failed);

    return params;
  }

}
