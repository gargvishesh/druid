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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A simple HashMap-based metadata manager for testing.
 */
public class InMemorySqlAsyncMetadataManager implements SqlAsyncMetadataManager
{
  private final Object lock = new Object();
  @GuardedBy("lock")
  private final Map<String, SqlAsyncQueryDetailsAndMetadata> queries = new HashMap<>();

  @Override
  public void addNewQuery(SqlAsyncQueryDetails queryDetails) throws AsyncQueryAlreadyExistsException
  {
    synchronized (lock) {
      if (queries.containsKey(queryDetails.getAsyncResultId())) {
        throw new AsyncQueryAlreadyExistsException(queryDetails.getAsyncResultId());
      }
      SqlAsyncQueryMetadata metadata = new SqlAsyncQueryMetadata(System.currentTimeMillis());
      queries.put(queryDetails.getAsyncResultId(), new SqlAsyncQueryDetailsAndMetadata(queryDetails, metadata));
    }
  }

  @Override
  public void updateQueryDetails(SqlAsyncQueryDetails queryDetails) throws AsyncQueryDoesNotExistException
  {
    synchronized (lock) {
      if (!queries.containsKey(queryDetails.getAsyncResultId())) {
        throw new AsyncQueryDoesNotExistException(queryDetails.getAsyncResultId());
      }
      SqlAsyncQueryMetadata metadata = new SqlAsyncQueryMetadata(System.currentTimeMillis());
      queries.put(queryDetails.getAsyncResultId(), new SqlAsyncQueryDetailsAndMetadata(queryDetails, metadata));
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
      if (queries.containsKey(asyncResultId)) {
        return Optional.of(queries.get(asyncResultId).getSqlAsyncQueryDetails());
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(String asyncResultId)
  {
    synchronized (lock) {
      if (queries.containsKey(asyncResultId)) {
        return Optional.of(queries.get(asyncResultId));
      } else {
        return Optional.empty();
      }
    }
  }

  @Override
  public Collection<String> getAllAsyncResultIds()
  {
    synchronized (lock) {
      return queries.keySet();
    }
  }
}
