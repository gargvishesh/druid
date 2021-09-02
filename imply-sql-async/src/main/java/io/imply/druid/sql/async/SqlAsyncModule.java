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
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.sql.guice.SqlModule;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SqlAsyncModule implements Module
{
  private static final Logger LOG = new Logger(SqlAsyncModule.class);

  public static final String ASYNC_ENABLED_KEY = "druid.sql.async.enabled";
  static final String ASYNC_BROKER_ID = "asyncBrokerId";

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
    if (isSqlEnabled(props) && isJsonOverHttpEnabled(props) && isAsyncEnabled(props)) {
      bindAsyncMetadataManager(binder);
      bindAsyncStorage(binder);

      Jerseys.addResource(binder, SqlAsyncResource.class);
      Jerseys.addResource(binder, SqlAsyncResultsMessageBodyWriter.class);

      // Force eager initialization.
      LifecycleModule.register(binder, SqlAsyncResource.class);

      binder.bind(SqlAsyncQueryPool.class).toProvider(SqlAsyncQueryPoolProvider.class).in(LazySingleton.class);

      JsonConfigProvider.bind(binder, "druid.sql.async.limit", AsyncQueryLimitsConfig.class);
    }
  }

  public static class SqlAsyncQueryPoolProvider implements Provider<SqlAsyncQueryPool>
  {
    private final SqlAsyncMetadataManager metadataManager;
    private final SqlAsyncResultManager resultManager;
    private final ObjectMapper jsonMapper;
    private final AsyncQueryLimitsConfig asyncQueryLimitsConfig;
    private final Lifecycle lifecycle;

    @Inject
    public SqlAsyncQueryPoolProvider(
        final SqlAsyncMetadataManager metadataManager,
        final SqlAsyncResultManager resultManager,
        @Json ObjectMapper jsonMapper,
        AsyncQueryLimitsConfig asyncQueryLimitsConfig,
        Lifecycle lifecycle
    )
    {
      this.metadataManager = metadataManager;
      this.resultManager = resultManager;
      this.jsonMapper = jsonMapper;
      this.asyncQueryLimitsConfig = asyncQueryLimitsConfig;
      this.lifecycle = lifecycle;
    }

    @Override
    public SqlAsyncQueryPool get()
    {
      final ExecutorService exec = new ThreadPoolExecutor(
          asyncQueryLimitsConfig.getMaxSimultaneousQuery(),
          // The maximum number of simultaneous query allowed is control by setting maximumPoolSize of the
          // ThreadPoolExecutor. This basically control how many query can be executing at the same time.
          asyncQueryLimitsConfig.getMaxSimultaneousQuery(),
          0L,
          TimeUnit.MILLISECONDS,
          // The queue limit is control by setting the size of the queue use for holding tasks before they are executed
          new LinkedBlockingQueue<>(asyncQueryLimitsConfig.getMaxQueryQueueSize()),
          Execs.makeThreadFactory("sql-async-pool-%d", null),
          new RejectedExecutionHandler()
          {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
              throw new QueryCapacityExceededException(asyncQueryLimitsConfig.getMaxQueryQueueSize());
            }
          }
      );
      SqlAsyncQueryPool sqlAsyncQueryPool = new SqlAsyncQueryPool(exec, metadataManager, resultManager, asyncQueryLimitsConfig, jsonMapper);
      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
            }

            @Override
            public void stop()
            {
              sqlAsyncQueryPool.shutdownNow();
            }
          }
      );
      LOG.debug(
          "Created SqlAsyncQueryPool with maxSimultaneousQuery[%d] and maxQueryQueueSize[%d]",
          asyncQueryLimitsConfig.getMaxSimultaneousQuery(),
          asyncQueryLimitsConfig.getMaxQueryQueueSize()
      );
      return sqlAsyncQueryPool;
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
  public static boolean isSqlEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE, "true"));
  }

  /**
   * This method must match to {@link SqlModule#isJsonOverHttpEnabled()}.
   */
  public static boolean isJsonOverHttpEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  public static boolean isAsyncEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(ASYNC_ENABLED_KEY, "false"));
  }

  public static void bindAsyncStorage(Binder binder)
  {
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
  }

  public static void bindAsyncMetadataManager(Binder binder)
  {
    binder.bind(SqlAsyncMetadataManager.class).to(CuratorSqlAsyncMetadataManager.class);
    binder.bind(CuratorSqlAsyncMetadataManager.class).in(LazySingleton.class);
  }
}
