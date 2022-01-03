/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;

import java.util.Collection;
import java.util.Optional;

public interface SqlAsyncMetadataManager
{
  void addNewQuery(SqlAsyncQueryDetails queryDetails) throws AsyncQueryAlreadyExistsException;

  /**
   * Atomically update the existing queryDetails to the given new queryDetails. It can throw RuntimeException
   * when the update fails with errors.
   *
   * @return true if the update succeeds. false if the update fails because the query is already in a final state.
   * @throws AsyncQueryDoesNotExistException if there is no such existing queryDetails for the asyncResultId
   *                                         of the given queryDetails.
   */
  boolean updateQueryDetails(SqlAsyncQueryDetails newQueryDetails) throws AsyncQueryDoesNotExistException;

  boolean removeQueryDetails(SqlAsyncQueryDetails queryDetails);

  Optional<SqlAsyncQueryDetails> getQueryDetails(String asyncResultId);

  Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(String asyncResultId);

  Collection<String> getAllAsyncResultIds();

  long totalCompleteQueryResultsSize();

  long totalCompleteQueryResultsSize(Collection<String> asyncResultIds);

  /**
   * Updates the lastUpdateTime for the query in the metadata.
   *
   * @param asyncResultId the id of the async query result
   *
   * @throws AsyncQueryDoesNotExistException when the query does not exist for the given ID.
   */
  void touchQueryLastUpdateTime(String asyncResultId) throws AsyncQueryDoesNotExistException;
}
