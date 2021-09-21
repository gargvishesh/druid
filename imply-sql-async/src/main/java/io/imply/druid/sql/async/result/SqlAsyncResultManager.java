/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.result;

import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Optional;

/**
 * Result manager for SQL async APIs.
 *
 * Design note:
 * We currently have some Imply-only interfaces for SQL async downloads like this interface, which is the reason for
 * why we have async downloads implementation in Druid core, not in an extension. We should think about this design
 * again because of these reasons.
 *
 * - This interface should be implemented per deep storage type. Whenever a new deep storage type is added by
 *   community, we should implement this interface for the new storage type by ourselves.
 * - If these interfaces were not Imply-only, the async downloads implementation can be contained in an extension.
 *
 * A potentially better alternative could be merging these Imply-only interfaces into the interfaces for deep storage
 * and contributing that change to the community. We should consider doing this before we support other deep storage
 * types than s3.
 */
public interface SqlAsyncResultManager
{
  OutputStream writeResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  Optional<SqlAsyncResults> readResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  /**
   * Delete results from storage of the given {@param asyncResultId}. This method is idempotent.
   * If the result of {@param asyncResultId} does not exist then this method simply return true.
   * This method doesn't cancel active result writing using {@link #writeResults}.
   *
   * @param asyncResultId of the async result to be deleted
   * @return true if the result of the given {@param asyncResultId} is successfully deleted or does not exist; false otherwise
   *
   */
  boolean deleteResults(String asyncResultId);

  /**
   * Retrieve all async result ids with existing result stored in storage.
   * The returned collection should include all asyncResultIds in the result storage even if their metadata
   * doesn't exist {@link io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager}, so that they can be cleaned up.
   * This method can return asyncResultIds of the queries that are still writing results using {@link #writeResults}.
   *
   * @return async result ids
   */
  Collection<String> getAllAsyncResultIds();
}
