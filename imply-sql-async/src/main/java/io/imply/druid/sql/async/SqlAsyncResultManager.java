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
import java.util.Optional;

public interface SqlAsyncResultManager
{
  OutputStream writeResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  Optional<SqlAsyncResults> readResults(SqlAsyncQueryDetails queryDetails) throws IOException;

  // TODO(gianm): Actually call this and clean stuff up
  void deleteResults(String SqlQueryId);
}
