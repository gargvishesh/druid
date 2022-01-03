/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.imply.druid.sql.async.discovery.BrokerIdService;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.query.SqlAsyncQueryStatsMonitor;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import io.imply.druid.sql.async.result.SqlAsyncResultsMessageBodyWriter;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.sql.guice.SqlModule;

import java.time.Clock;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class SqlAsyncModule implements DruidModule
{
  public static final String BASE_ASYNC_CONFIG_KEY = "druid.query.async";
  public static final String ASYNC_BROKER_ID = "asyncBrokerId";

  private static final Logger LOG = new Logger(SqlAsyncModule.class);

  @Inject
  public void init(Properties properties)
  {
    if (!isSqlEnabled(properties) || !isJsonOverHttpEnabled(properties)) {
      throw new ISE(
          "Both %s and %s should be set to true",
          SqlModule.PROPERTY_SQL_ENABLE,
          SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP
      );
    }
  }

  @Override
  public void configure(Binder binder)
  {
    bindAsyncLimitsConfig(binder);

    Jerseys.addResource(binder, SqlAsyncResource.class);
    Jerseys.addResource(binder, SqlAsyncResultsMessageBodyWriter.class);

    // Force eager initialization.
    LifecycleModule.register(binder, SqlAsyncResource.class);

    binder.bind(SqlAsyncQueryPool.class).toProvider(SqlAsyncQueryPoolProvider.class).in(LazySingleton.class);
    //this is to enable better testing by allowing control over clocks in tests
    binder.bind(Clock.class).toInstance(Clock.systemUTC());
    binder.bind(SqlAsyncQueryStatsMonitor.class).in(LazySingleton.class);
    MetricsModule.register(binder, SqlAsyncQueryStatsMonitor.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  public static class SqlAsyncQueryPoolProvider implements Provider<SqlAsyncQueryPool>
  {
    private final String brokerId;
    private final AsyncQueryConfig asyncQueryConfig;
    private final SqlAsyncMetadataManager metadataManager;
    private final SqlAsyncResultManager resultManager;
    private final ObjectMapper jsonMapper;
    private final Lifecycle lifecycle;
    private final SqlAsyncLifecycleManager sqlAsyncLifecycleManager;

    @Inject
    public SqlAsyncQueryPoolProvider(
        @Named(ASYNC_BROKER_ID) final String brokerId,
        final @Json ObjectMapper jsonMapper,
        final AsyncQueryConfig asyncQueryConfig,
        final SqlAsyncMetadataManager metadataManager,
        final SqlAsyncResultManager resultManager,
        final SqlAsyncLifecycleManager sqlAsyncLifecycleManager,
        final Lifecycle lifecycle
    )
    {
      this.brokerId = brokerId;
      this.asyncQueryConfig = asyncQueryConfig;
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
          asyncQueryConfig.getMaxConcurrentQueries(),
          // The maximum number of concurrent query allowed is control by setting maximumPoolSize of the
          // ThreadPoolExecutor. This basically control how many query can be executing at the same time.
          asyncQueryConfig.getMaxConcurrentQueries(),
          0L,
          TimeUnit.MILLISECONDS,
          // The queue limit is control by setting the size of the queue use for holding tasks before they are executed
          new LinkedBlockingQueue<>(asyncQueryConfig.getMaxQueriesToQueue()),
          Execs.makeThreadFactory("sql-async-pool-%d", null),
          new RejectedExecutionHandler()
          {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
              throw new QueryCapacityExceededException(asyncQueryConfig.getMaxQueriesToQueue());
            }
          }
      );
      SqlAsyncQueryPool sqlAsyncQueryPool = new SqlAsyncQueryPool(
          brokerId,
          exec,
          metadataManager,
          resultManager,
          sqlAsyncLifecycleManager,
          jsonMapper

      );
      lifecycle.addManagedInstance(sqlAsyncQueryPool);
      LOG.debug(
          "Created SqlAsyncQueryPool with maxConcurrentQueries[%d] and maxQueriesToQueue[%d]",
          asyncQueryConfig.getMaxConcurrentQueries(),
          asyncQueryConfig.getMaxQueriesToQueue()
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

  public static void bindAsyncLimitsConfig(Binder binder)
  {
    JsonConfigProvider.bind(binder, BASE_ASYNC_CONFIG_KEY, AsyncQueryConfig.class);
  }
}
