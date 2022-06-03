/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;

import java.time.Clock;

public class AsyncQueryContext
{
  final String brokerId;
  final SqlAsyncQueryPool queryPool;
  final SqlAsyncMetadataManager metadataManager;
  final SqlAsyncResultManager resultManager;
  final SqlLifecycleFactory sqlLifecycleFactory;
  final SqlAsyncLifecycleManager sqlAsyncLifecycleManager;
  final AuthorizerMapper authorizerMapper;
  final ObjectMapper jsonMapper;
  final Clock clock;
  final AsyncQueryConfig asyncQueryReadRefreshConfig;

  @Inject
  public AsyncQueryContext(
      @Named(SqlAsyncModule.ASYNC_BROKER_ID) final String brokerId,
      final SqlAsyncQueryPool queryPool,
      final SqlAsyncMetadataManager metadataManager,
      final SqlAsyncResultManager resultManager,
      final SqlLifecycleFactory sqlLifecycleFactory,
      final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
      final AuthorizerMapper authorizerMapper,
      @Json final ObjectMapper jsonMapper,
      final AsyncQueryConfig asyncQueryReadRefreshConfig,
      final Clock clock
  )
  {
    this.brokerId = brokerId;
    this.queryPool = queryPool;
    this.metadataManager = metadataManager;
    this.resultManager = resultManager;
    this.sqlLifecycleFactory = sqlLifecycleFactory;
    this.sqlAsyncLifecycleManager = sqlAsyncLifecycleManager;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
    this.asyncQueryReadRefreshConfig = asyncQueryReadRefreshConfig;
    this.clock = clock;
  }
}
