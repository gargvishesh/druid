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
      = CoordinatorStat.toDebugAndEmit("metadataCleanupSuccess", "async/cleanup/metadata/removed/count");
  static final CoordinatorStat METADATA_CLEANUP_FAILED
      = CoordinatorStat.toDebugAndEmit("metadataCleanupFailed", "async/cleanup/metadata/failed/count");
  static final CoordinatorStat METADATA_CLEANUP_SKIPPED
      = CoordinatorStat.toDebugAndEmit("metadataCleanupSkipped", "async/cleanup/metadata/skipped/count");

  static final CoordinatorStat RESULT_CLEANUP_SUCCESS_COUNT
      = CoordinatorStat.toDebugAndEmit("resultCleanupSuccessCount", "async/cleanup/result/removed/count");
  static final CoordinatorStat RESULT_CLEANUP_SUCCESS_BYTES
      = CoordinatorStat.toDebugAndEmit("resultCleanupSuccessBytes", "async/cleanup/result/removed/bytes");
  static final CoordinatorStat RESULT_CLEANUP_FAILED_COUNT
      = CoordinatorStat.toDebugAndEmit("resultRemovalFailedCount", "async/cleanup/result/failed/count");
  static final CoordinatorStat RESULT_CLEANUP_FAILED_BYTES
      = CoordinatorStat.toDebugAndEmit("resultRemovedFailedSize", "async/cleanup/result/failed/bytes");

  static final CoordinatorStat UNDETERMINED_QUERIES
      = CoordinatorStat.toDebugAndEmit("queryUndetermined", "async/query/undetermined/count");

  static final CoordinatorStat TRACKED_RESULTS
      = CoordinatorStat.toDebugAndEmit("trackedResults", "async/result/tracked/count");
  static final CoordinatorStat TRACKED_RESULT_BYTES
      = CoordinatorStat.toDebugAndEmit("trackedResultBytes", "async/result/tracked/bytes");
}
