/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface SqlAsyncMetadataManager
{
  void addNewQuery(SqlAsyncQueryDetails queryDetails) throws IOException, AsyncQueryAlreadyExistsException;

  void updateQueryDetails(SqlAsyncQueryDetails queryDetails) throws IOException, AsyncQueryDoesNotExistException;

  // TODO(gianm): Actually call this and clean stuff up
  boolean removeQueryDetails(SqlAsyncQueryDetails queryDetails) throws IOException;

  Optional<SqlAsyncQueryDetails> getQueryDetails(String asyncResultId) throws IOException;

  Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(String asyncResultId) throws IOException;

  Collection<String> getAllAsyncResultIds() throws IOException;

  long totalCompleteQueryResultsSize() throws IOException;
}
