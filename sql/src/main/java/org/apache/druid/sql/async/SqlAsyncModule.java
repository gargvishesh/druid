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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.util.concurrent.ExecutorService;

public class SqlAsyncModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(SqlAsyncResultManager.class).to(LocalSqlAsyncResultManager.class);
    binder.bind(SqlAsyncMetadataManager.class).to(CuratorSqlAsyncMetadataManager.class);

    binder.bind(LocalSqlAsyncResultManager.class).in(LazySingleton.class);
    binder.bind(CuratorSqlAsyncMetadataManager.class).in(LazySingleton.class);

    Jerseys.addResource(binder, SqlAsyncResource.class);
  }

  @Provides
  @LazySingleton
  public SqlAsyncQueryPool createQueryPool(
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      @Json ObjectMapper jsonMapper
  )
  {
    // TODO(gianm): Limit concurrency somehow on the executor service
    final ExecutorService exec = Execs.multiThreaded(4, "sql-async-pool-%d");
    return new SqlAsyncQueryPool(exec, metadataManager, resultManager, jsonMapper);
  }
}
