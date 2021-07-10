/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.async;

import org.apache.druid.java.util.common.IOE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO(gianm): Make not in-memory...
 */
public class LocalSqlAsyncResultManager implements SqlAsyncResultManager
{
  private final ConcurrentMap<String, ByteArrayOutputStream> resultMap;

  public LocalSqlAsyncResultManager()
  {
    this.resultMap = new ConcurrentHashMap<>();
  }

  @Override
  public OutputStream writeResults(final String sqlQueryId) throws IOException
  {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();

    if (resultMap.putIfAbsent(sqlQueryId, stream) != null) {
      throw new IOE("Query [%s] result stream already exists", sqlQueryId);
    }

    return stream;
  }

  @Override
  public Optional<SqlAsyncResults> readResults(final String sqlQueryId)
  {
    return Optional.ofNullable(resultMap.get(sqlQueryId)).map(
        stream -> {
          final byte[] bytes = stream.toByteArray();
          return new SqlAsyncResults(new ByteArrayInputStream(bytes), bytes.length);
        }
    );
  }

  @Override
  public void deleteResults(final String sqlQueryId)
  {
    // TODO(gianm): Call this in a way that doesn't cause problems when two users happen to issue queries with
    //  the same manually-specified ID
    resultMap.remove(sqlQueryId);
  }
}
