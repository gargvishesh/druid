/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.imply.druid.sql.async.SqlAsyncModule;
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryMetadata;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.sql.guice.SqlModule;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class SqlAsyncCleanupModuleTest
{
  @Test
  public void testDisableAsyncQuery() throws Exception
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "false");
    Injector injector = makeInjector(properties);
    Assert.assertThrows(
        "Guice configuration errors",
        ConfigurationException.class,
        () -> injector.getInstance(SqlAsyncMetadataManager.class)
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    KillAsyncQueryMetadata cleanup = new KillAsyncQueryMetadata(Duration.standardDays(1), null);
    final byte[] bytes = objectMapper.writeValueAsBytes(cleanup);
    Assert.assertThrows(
        "Jackson configuration errors",
        InvalidTypeIdException.class,
        () -> objectMapper.readValue(bytes, CoordinatorCustomDuty.class)
    );
  }

  @Test
  public void testEnableAsyncQuery() throws Exception
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "true");
    Injector injector = makeInjector(properties);
    Assert.assertNotNull(injector.getInstance(SqlAsyncMetadataManager.class));

    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(SqlAsyncMetadataManager.class, null);
    String json = StringUtils.format(
        "{\"type\":\"%s\", \"%s\":\"PT30S\"}",
        KillAsyncQueryMetadata.JSON_TYPE_NAME,
        KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY);
    CoordinatorCustomDuty customDuty = objectMapper.setInjectableValues(injectableValues).readValue(json, CoordinatorCustomDuty.class);
    Assert.assertTrue(customDuty instanceof KillAsyncQueryMetadata);
  }

  private Injector makeInjector(Properties props)
  {
    Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new JacksonModule(),
        new CuratorModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>(){}).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(null)));
        },
        new SqlAsyncCleanupModule(props)
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    for (Module jacksonModule : new SqlAsyncCleanupModule(props).getJacksonModules()) {
      objectMapper.registerModule(jacksonModule);
    }
    return injector;
  }
}
