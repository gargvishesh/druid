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

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface SqlAsyncMetadataManager
{
  void addNewQuery(SqlAsyncQueryDetails queryDetails) throws IOException, AsyncQueryAlreadyExistsException;

  // TODO(jihoon): this method is racy because query state can be updated by both coordinator and broker.
  //               should add compareAndSwap() instead.
  void updateQueryDetails(SqlAsyncQueryDetails queryDetails) throws IOException, AsyncQueryDoesNotExistException;

  // TODO(gianm): Actually call this and clean stuff up
  boolean removeQueryDetails(SqlAsyncQueryDetails queryDetails) throws IOException;

  Optional<SqlAsyncQueryDetails> getQueryDetails(String asyncResultId) throws IOException;

  Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(String asyncResultId) throws IOException;

  Collection<String> getAllAsyncResultIds() throws IOException;

  long totalCompleteQueryResultsSize() throws IOException;

  long totalCompleteQueryResultsSize(Collection<String> asyncResultIds) throws IOException;
}
