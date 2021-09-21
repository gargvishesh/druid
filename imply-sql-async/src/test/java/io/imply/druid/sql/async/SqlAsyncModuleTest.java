/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.result.LocalSqlAsyncResultManager;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.sql.guice.SqlModule;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class SqlAsyncModuleTest
{
  @Test
  public void testDisableAsyncQuery()
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "false");
    Injector injector = makeInjector(properties);
    Assert.assertThrows(
        "Guice configuration errors",
        ConfigurationException.class,
        () -> injector.getProvider(SqlAsyncQueryPool.class)
    );
  }

  @Test
  public void testEnableAsyncQuery()
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "true");
    properties.setProperty(
        SqlAsyncModule.STORAGE_TYPE_CONFIG_KEY,
        LocalSqlAsyncResultManager.LOCAL_RESULT_MANAGER_TYPE
    );
    properties.setProperty(
        LocalSqlAsyncResultManager.LOCAL_STORAGE_DIRECTORY_CONFIG_KEY,
        "test"
    );
    Injector injector = makeInjector(properties);
    Assert.assertNotNull(injector.getInstance(SqlAsyncMetadataManager.class));
    Assert.assertNotNull(injector.getInstance(SqlAsyncResultManager.class));
    Assert.assertNotNull(injector.getInstance(SqlAsyncQueryPool.class));
  }

  @Test
  public void testBrokerIdInjectedAndSingleton()
  {
    Injector injector = makeInjector(new Properties());
    final String brokerId = injector.getInstance(Key.get(String.class, Names.named(SqlAsyncModule.ASYNC_BROKER_ID)));
    Assert.assertNotNull(brokerId);
    final String brokerId2 = injector.getInstance(Key.get(String.class, Names.named(SqlAsyncModule.ASYNC_BROKER_ID)));
    Assert.assertSame(brokerId, brokerId2);
  }

  private Injector makeInjector(Properties props)
  {
    return Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new JacksonModule(),
        new CuratorModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(
              new TypeLiteral<Supplier<DefaultQueryConfig>>(){})
                .toInstance(Suppliers.ofInstance(new DefaultQueryConfig(null))
          );
        },
        new SqlAsyncModule(props)
    );
  }
}
