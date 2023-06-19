/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import org.apache.druid.server.coordinator.stats.CoordinatorStat;

/**
 * Stats emitted by the async query coordinator duties.
 */
class Stats
{
  static final CoordinatorStat METADATA_CLEANUP_SUCCESS
      = new CoordinatorStat("metadataCleanupSuccess", "async/cleanup/metadata/removed/count");
  static final CoordinatorStat METADATA_CLEANUP_FAILED
      = new CoordinatorStat("metadataCleanupFailed", "async/cleanup/metadata/failed/count");
  static final CoordinatorStat METADATA_CLEANUP_SKIPPED
      = new CoordinatorStat("metadataCleanupSkipped", "async/cleanup/metadata/skipped/count");

  static final CoordinatorStat RESULT_CLEANUP_SUCCESS_COUNT
      = new CoordinatorStat("resultCleanupSuccessCount", "async/cleanup/result/removed/count");
  static final CoordinatorStat RESULT_CLEANUP_SUCCESS_BYTES
      = new CoordinatorStat("resultCleanupSuccessBytes", "async/cleanup/result/removed/bytes");
  static final CoordinatorStat RESULT_CLEANUP_FAILED_COUNT
      = new CoordinatorStat("resultRemovalFailedCount", "async/cleanup/result/failed/count");
  static final CoordinatorStat RESULT_CLEANUP_FAILED_BYTES
      = new CoordinatorStat("resultRemovedFailedSize", "async/cleanup/result/failed/bytes");

  static final CoordinatorStat UNDETERMINED_QUERIES
      = new CoordinatorStat("queryUndetermined", "async/query/undetermined/count");

  static final CoordinatorStat TRACKED_RESULTS
      = new CoordinatorStat("trackedResults", "async/result/tracked/count");
  static final CoordinatorStat TRACKED_RESULT_BYTES
      = new CoordinatorStat("trackedResultBytes", "async/result/tracked/bytes");
}
