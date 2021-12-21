/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManager;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManagerConfig;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;

public class SqlAsyncCoreModule implements Module
{
  public static final String BASE_STORAGE_CONFIG_KEY = "druid.query.async.storage";
  public static final String STORAGE_TYPE_CONFIG_KEY = String.join(".", BASE_STORAGE_CONFIG_KEY, "type");

  @Override
  public void configure(Binder binder)
  {
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
}
