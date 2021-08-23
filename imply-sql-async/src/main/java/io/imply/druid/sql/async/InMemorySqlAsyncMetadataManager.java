/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemorySqlAsyncMetadataManager implements SqlAsyncMetadataManager
{
  private final Object lock = new Object();
  @GuardedBy("lock")
  private final Map<String, SqlAsyncQueryDetails> queries = new HashMap<>();

  @Override
  public void addNewQuery(SqlAsyncQueryDetails queryDetails) throws AsyncQueryAlreadyExistsException
  {
    synchronized (lock) {
      if (queries.containsKey(queryDetails.getAsyncResultId())) {
        throw new AsyncQueryAlreadyExistsException(queryDetails.getAsyncResultId());
      }
      queries.put(queryDetails.getAsyncResultId(), queryDetails);
    }
  }

  @Override
  public void updateQueryDetails(SqlAsyncQueryDetails queryDetails) throws AsyncQueryDoesNotExistException
  {
    synchronized (lock) {
      if (!queries.containsKey(queryDetails.getAsyncResultId())) {
        throw new AsyncQueryDoesNotExistException(queryDetails.getAsyncResultId());
      }
      queries.put(queryDetails.getAsyncResultId(), queryDetails);
    }
  }

  @Override
  public boolean removeQueryDetails(SqlAsyncQueryDetails queryDetails)
  {
    synchronized (lock) {
      return queries.remove(queryDetails.getAsyncResultId()) != null;
    }
  }

  @Override
  public Optional<SqlAsyncQueryDetails> getQueryDetails(String asyncResultId)
  {
    synchronized (lock) {
      return Optional.ofNullable(queries.get(asyncResultId));
    }
  }
}
