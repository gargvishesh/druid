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
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManagerImpl;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataStorageTableConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

/**
 * Common module for the coordiantor and broker. Binds the metadata manager and query result storage.
 */
@LoadScope(roles = {NodeRole.COORDINATOR_JSON_NAME, NodeRole.BROKER_JSON_NAME})
public class SqlAsyncMetadataModule implements DruidModule
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
    binder.bind(SqlAsyncMetadataManager.class).to(SqlAsyncMetadataManagerImpl.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.metadata.storage.tables", SqlAsyncMetadataStorageTableConfig.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }
}
