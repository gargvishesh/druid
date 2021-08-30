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
import java.io.OutputStream;
import java.util.Collection;
import java.util.Optional;

public interface SqlAsyncResultManager
{
  OutputStream writeResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  Optional<SqlAsyncResults> readResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  /**
   * Delete results from storage of the given {@param asyncResultId}. This method is idempotent.
   * If the result of {@param asyncResultId} does not exist then this method simply return true.
   *
   * @param asyncResultId of the async result to be deleted
   * @return true if the result of the given {@param asyncResultId} is successfully deleted or does not exist; false otherwise
   *
   */
  boolean deleteResults(String asyncResultId);

  /**
   * Retrieve all async result ids with existing result stored in storage
   *
   * @return async result ids
   *
   */
  Collection<String> getAllResults();
}
