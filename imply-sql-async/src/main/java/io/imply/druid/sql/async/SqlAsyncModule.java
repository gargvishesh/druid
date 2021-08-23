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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.sql.guice.SqlModule;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class SqlAsyncModule implements Module
{
  static final String ASYNC_BROKER_ID = "asyncBrokerId";
  static final String ASYNC_ENABLED_KEY = "druid.sql.async.enabled";

  private static final String LOCAL_RESULT_MANAGER_TYPE = "local";

  @Inject
  private Properties props;

  public SqlAsyncModule()
  {
  }

  @VisibleForTesting
  SqlAsyncModule(Properties props)
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
    if (isSqlEnabled() && isJsonOverHttpEnabled() && isAsyncEnabled()) {
      binder.bind(SqlAsyncMetadataManager.class).to(CuratorSqlAsyncMetadataManager.class);

      PolyBind.createChoice(
          binder,
          "druid.sql.asyncstorage.type",
          Key.get(SqlAsyncResultManager.class),
          Key.get(LocalSqlAsyncResultManager.class)
      );

      PolyBind.optionBinder(binder, Key.get(SqlAsyncResultManager.class))
              .addBinding(LOCAL_RESULT_MANAGER_TYPE)
              .to(LocalSqlAsyncResultManager.class)
              .in(LazySingleton.class);

      JsonConfigProvider.bind(binder, "druid.sql.asyncstorage.local", LocalSqlAsyncResultManagerConfig.class);

      binder.bind(CuratorSqlAsyncMetadataManager.class).in(LazySingleton.class);

      Jerseys.addResource(binder, SqlAsyncResource.class);
      Jerseys.addResource(binder, SqlAsyncResultsMessageBodyWriter.class);

      // Force eager initialization.
      LifecycleModule.register(binder, SqlAsyncResource.class);

      binder.bind(SqlAsyncQueryPool.class).toProvider(SqlAsyncQueryPoolProvider.class);
    }
  }

  public static class SqlAsyncQueryPoolProvider implements Provider<SqlAsyncQueryPool>
  {
    private final SqlAsyncMetadataManager metadataManager;
    private final SqlAsyncResultManager resultManager;
    private final ObjectMapper jsonMapper;

    @Inject
    public SqlAsyncQueryPoolProvider(
        final SqlAsyncMetadataManager metadataManager,
        final SqlAsyncResultManager resultManager,
        @Json ObjectMapper jsonMapper
    )
    {
      this.metadataManager = metadataManager;
      this.resultManager = resultManager;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public SqlAsyncQueryPool get()
    {
      // TODO(gianm): Limit concurrency somehow on the executor service
      final ExecutorService exec = Execs.multiThreaded(4, "sql-async-pool-%d");
      return new SqlAsyncQueryPool(exec, metadataManager, resultManager, jsonMapper);
    }
  }

  @Provides
  @LazySingleton
  @Named(ASYNC_BROKER_ID)
  public String getBrokerId()
  {
    return UUIDUtils.generateUuid();
  }

  /**
   * This method must match to {@link SqlModule#isEnabled()}.
   */
  private boolean isSqlEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE, "true"));
  }

  /**
   * This method must match to {@link SqlModule#isJsonOverHttpEnabled()}.
   */
  private boolean isJsonOverHttpEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  private boolean isAsyncEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(ASYNC_ENABLED_KEY, "false"));
  }
}
