/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.sql.async.SqlAsyncModule;
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryMetadata;
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryResultWithoutMetadata;
import org.apache.druid.initialization.DruidModule;

import java.util.List;
import java.util.Properties;

public class SqlAsyncCleanupModule implements DruidModule
{
  @Inject
  private Properties props;

  public SqlAsyncCleanupModule()
  {
  }

  @VisibleForTesting
  SqlAsyncCleanupModule(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (SqlAsyncModule.isSqlEnabled(props) && SqlAsyncModule.isJsonOverHttpEnabled(props) && SqlAsyncModule.isAsyncEnabled(props)) {
      return ImmutableList.<Module>of(
          new SimpleModule("SqlAsyncCleanupModule")
              .registerSubtypes(KillAsyncQueryMetadata.class, KillAsyncQueryResultWithoutMetadata.class)
      );
    } else {
      return ImmutableList.of();
    }
  }

  @Override
  public void configure(Binder binder)
  {
    if (SqlAsyncModule.isSqlEnabled(props) && SqlAsyncModule.isJsonOverHttpEnabled(props) && SqlAsyncModule.isAsyncEnabled(props)) {
      SqlAsyncModule.bindAsyncMetadataManager(binder);
      SqlAsyncModule.bindAsyncStorage(binder);
    }
  }
}
