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
import io.imply.druid.sql.async.discovery.BrokerIdService;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManagerImpl;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataStorageTableConfig;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.query.SqlAsyncQueryStatsMonitor;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManager;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManagerConfig;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import io.imply.druid.sql.async.result.SqlAsyncResultsMessageBodyWriter;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.sql.guice.SqlModule;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SqlAsyncModule implements Module
{
  public static final String BASE_ASYNC_CONFIG_KEY = "druid.query.async";
  public static final String ASYNC_ENABLED_KEY = String.join(".", BASE_ASYNC_CONFIG_KEY, "sql", "enabled");
  public static final String BASE_STORAGE_CONFIG_KEY = String.join(".", BASE_ASYNC_CONFIG_KEY, "storage");
  public static final String STORAGE_TYPE_CONFIG_KEY = String.join(".", BASE_STORAGE_CONFIG_KEY, "type");
  public static final String ASYNC_BROKER_ID = "asyncBrokerId";

  private static final Logger LOG = new Logger(SqlAsyncModule.class);

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
    if (isEnabled(props)) {
      bindAsyncMetadataManager(binder);
      bindAsyncStorage(binder);
      bindAsyncLimitsConfig(binder);

      Jerseys.addResource(binder, SqlAsyncResource.class);
      Jerseys.addResource(binder, SqlAsyncResultsMessageBodyWriter.class);

      // Force eager initialization.
      LifecycleModule.register(binder, SqlAsyncResource.class);

      binder.bind(SqlAsyncQueryPool.class).toProvider(SqlAsyncQueryPoolProvider.class).in(LazySingleton.class);
      binder.bind(SqlAsyncQueryStatsMonitor.class).in(LazySingleton.class);
      MetricsModule.register(binder, SqlAsyncQueryStatsMonitor.class);
    }
  }

  public static class SqlAsyncQueryPoolProvider implements Provider<SqlAsyncQueryPool>
  {
    private final String brokerId;
    private final AsyncQueryPoolConfig asyncQueryPoolConfig;
    private final SqlAsyncMetadataManager metadataManager;
    private final SqlAsyncResultManager resultManager;
    private final ObjectMapper jsonMapper;
    private final Lifecycle lifecycle;
    private final SqlAsyncLifecycleManager sqlAsyncLifecycleManager;

    @Inject
    public SqlAsyncQueryPoolProvider(
        @Named(ASYNC_BROKER_ID) final String brokerId,
        final @Json ObjectMapper jsonMapper,
        final AsyncQueryPoolConfig asyncQueryPoolConfig,
        final SqlAsyncMetadataManager metadataManager,
        final SqlAsyncResultManager resultManager,
        final AsyncQueryPoolConfig asyncQueryLimitsConfig,
        final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
        final Lifecycle lifecycle
    )
    {
      this.brokerId = brokerId;
      this.asyncQueryPoolConfig = asyncQueryPoolConfig;
      this.metadataManager = metadataManager;
      this.resultManager = resultManager;
      this.jsonMapper = jsonMapper;
      this.sqlAsyncLifecycleManager = sqlAsyncLifecycleManager;
      this.lifecycle = lifecycle;
    }

    @Override
    public SqlAsyncQueryPool get()
    {
      final ThreadPoolExecutor exec = new ThreadPoolExecutor(
          asyncQueryPoolConfig.getMaxConcurrentQueries(),
          // The maximum number of concurrent query allowed is control by setting maximumPoolSize of the
          // ThreadPoolExecutor. This basically control how many query can be executing at the same time.
          asyncQueryPoolConfig.getMaxConcurrentQueries(),
          0L,
          TimeUnit.MILLISECONDS,
          // The queue limit is control by setting the size of the queue use for holding tasks before they are executed
          new LinkedBlockingQueue<>(asyncQueryPoolConfig.getMaxQueriesToQueue()),
          Execs.makeThreadFactory("sql-async-pool-%d", null),
          new RejectedExecutionHandler()
          {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
              throw new QueryCapacityExceededException(asyncQueryPoolConfig.getMaxQueriesToQueue());
            }
          }
      );
      SqlAsyncQueryPool sqlAsyncQueryPool = new SqlAsyncQueryPool(
          brokerId,
          asyncQueryPoolConfig,
          exec,
          metadataManager,
          resultManager,
          sqlAsyncLifecycleManager,
          jsonMapper

      );
      lifecycle.addManagedInstance(sqlAsyncQueryPool);
      LOG.debug(
          "Created SqlAsyncQueryPool with maxConcurrentQueries[%d] and maxQueriesToQueue[%d]",
          asyncQueryPoolConfig.getMaxConcurrentQueries(),
          asyncQueryPoolConfig.getMaxQueriesToQueue()
      );
      return sqlAsyncQueryPool;
    }
  }

  @Provides
  @LazySingleton
  @Named(ASYNC_BROKER_ID)
  public String getBrokerId()
  {
    // The UUID generated should never have SqlAsyncUtil.BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR charater
    // Hence, the removing should never do anything and is just a safety check
    return StringUtils.removeChar(UUIDUtils.generateUuid(), SqlAsyncUtil.BROKER_ID_AND_SQL_QUERY_ID_SEPARATOR_CHAR);
  }

  @Provides
  @LazySingleton
  public BrokerIdService getBrokerIdService(@Named(ASYNC_BROKER_ID) String brokerId)
  {
    return new BrokerIdService(brokerId);
  }

  /**
   * This method must match to {@link SqlModule#isEnabled()}.
   */
  private static boolean isSqlEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE, "true"));
  }

  /**
   * This method must match to {@link SqlModule#isJsonOverHttpEnabled()}.
   */
  private static boolean isJsonOverHttpEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  private static boolean isAsyncEnabled(Properties props)
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(ASYNC_ENABLED_KEY, "false"));
  }

  public static boolean isEnabled(Properties props)
  {
    return isSqlEnabled(props) && isJsonOverHttpEnabled(props) && isAsyncEnabled(props);
  }

  public static void bindAsyncStorage(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        STORAGE_TYPE_CONFIG_KEY,
        Key.get(SqlAsyncResultManager.class),
        Key.get(LocalSqlAsyncResultManager.class)
    );

    PolyBind.optionBinder(binder, Key.get(SqlAsyncResultManager.class))
            .addBinding(LocalSqlAsyncResultManager.LOCAL_RESULT_MANAGER_TYPE)
            .to(LocalSqlAsyncResultManager.class)
            .in(LazySingleton.class);

    JsonConfigProvider.bind(
        binder,
        LocalSqlAsyncResultManager.BASE_LOCAL_STORAGE_CONFIG_KEY,
        LocalSqlAsyncResultManagerConfig.class
    );
  }

  public static void bindAsyncMetadataManager(Binder binder)
  {
    binder.bind(SqlAsyncMetadataManager.class).to(SqlAsyncMetadataManagerImpl.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.metadata.storage.tables", SqlAsyncMetadataStorageTableConfig.class);
  }

  public static void bindAsyncLimitsConfig(Binder binder)
  {
    JsonConfigProvider.bind(binder, BASE_ASYNC_CONFIG_KEY, AsyncQueryPoolConfig.class);
  }
}
