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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManagerImpl;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataStorageTableConfig;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManager;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManagerConfig;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

/**
 * Common module for the coordiantor and broker. Binds the metadata manager and query result storage.
 */
@LoadScope(roles = {NodeRole.COORDINATOR_JSON_NAME, NodeRole.BROKER_JSON_NAME})
public class SqlAsyncCommonModule implements DruidModule
{
  public static final String BASE_STORAGE_CONFIG_KEY = String.join(
      ".",
      SqlAsyncModule.BASE_ASYNC_CONFIG_KEY,
      "storage"
  );
  public static final String STORAGE_TYPE_CONFIG_KEY = String.join(".", BASE_STORAGE_CONFIG_KEY, "type");

  @Override
  public void configure(Binder binder)
  {
    bindAsyncMetadataManager(binder);
    bindAsyncStorage(binder);
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

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }
}
