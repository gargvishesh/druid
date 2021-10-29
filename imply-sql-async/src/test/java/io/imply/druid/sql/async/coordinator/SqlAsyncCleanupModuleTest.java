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
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryResultWithoutMetadata;
import io.imply.druid.sql.async.coordinator.duty.UpdateStaleQueryState;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.druid.sql.guice.SqlModule;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class SqlAsyncCleanupModuleTest
{
  @Rule
  public DerbyConnectorRule connectorRule = new DerbyConnectorRule();

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
        KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY
    );
    CoordinatorCustomDuty customDuty = objectMapper.setInjectableValues(injectableValues)
                                                   .readValue(json, CoordinatorCustomDuty.class);
    Assert.assertTrue(customDuty instanceof KillAsyncQueryMetadata);
  }

  @Test
  public void testConfigureDetaultCleanupDutyGroup() throws Exception
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "true");
    SqlAsyncCleanupModule.setupAsyncCleanupCoordinatorDutyGroup("testGroupName", new DefaultObjectMapper(), properties);

    Assert.assertEquals(
        StringUtils.format(
            "[\"%s\",\"%s\",\"%s\"]",
            KillAsyncQueryMetadata.JSON_TYPE_NAME,
            KillAsyncQueryResultWithoutMetadata.JSON_TYPE_NAME,
            UpdateStaleQueryState.TYPE
        ),
        properties.getProperty("druid.coordinator.testGroupName.duties")
    );
    Assert.assertEquals(
        "PT60S",
        properties.getProperty(
            StringUtils.format(
                "druid.coordinator.testGroupName.duty.%s.%s",
                KillAsyncQueryMetadata.JSON_TYPE_NAME,
                KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY
            )
        )
    );
    Assert.assertNull(
        properties.getProperty(
            StringUtils.format(
                "druid.coordinator.testGroupName.duty.%s.%s",
                UpdateStaleQueryState.TYPE,
                UpdateStaleQueryState.TIME_TO_WAIT_AFTER_BROKER_GONE
            )
        )
    );
    Assert.assertEquals(
        "PT30S",
        properties.getProperty("druid.coordinator.testGroupName.period")
    );
  }

  @Test
  public void testConfigureCleanupDutyGroup() throws Exception
  {
    Properties properties = new Properties();
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    properties.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    properties.setProperty(SqlAsyncModule.ASYNC_ENABLED_KEY, "true");
    properties.setProperty(SqlAsyncCleanupModule.CLEANUP_POLL_PERIOD_CONFIG_KEY, "PT10S");
    properties.setProperty(SqlAsyncCleanupModule.CLEANUP_TIME_TO_RETAIN_CONFIG_KEY, "PT20S");
    properties.setProperty(SqlAsyncCleanupModule.TIME_TO_WAIT_AFTER_BROKER_GONE, "PT40S");

    SqlAsyncCleanupModule.setupAsyncCleanupCoordinatorDutyGroup("testGroupName", new DefaultObjectMapper(), properties);

    Assert.assertEquals(
        "[\"killAsyncQueryMetadata\",\"killAsyncQueryResultWithoutMetadata\",\"updateStaleQueryState\"]",
        properties.getProperty("druid.coordinator.testGroupName.duties")
    );
    Assert.assertEquals(
        "PT20S",
        properties.getProperty("druid.coordinator.testGroupName.duty.killAsyncQueryMetadata.timeToRetain")
    );
    Assert.assertEquals(
        "PT40S",
        properties.getProperty("druid.coordinator.testGroupName.duty.updateStaleQueryState.timeToWaitAfterBrokerGone")
    );
    Assert.assertEquals(
        "PT10S",
        properties.getProperty("druid.coordinator.testGroupName.period")
    );
  }

  private Injector makeInjector(Properties props)
  {
    SqlAsyncCleanupModule cleanupModule = new SqlAsyncCleanupModule(props, new DefaultObjectMapper());
    Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new JacksonModule(),
        new CuratorModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(new TypeLiteral<Supplier<MetadataStorageConnectorConfig>>(){})
                .toInstance(Suppliers.ofInstance(new MetadataStorageConnectorConfig()));
          binder.bind(SQLMetadataConnector.class).toInstance(connectorRule.getConnector());
        },
        cleanupModule
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    for (Module jacksonModule : cleanupModule.getJacksonModules()) {
      objectMapper.registerModule(jacksonModule);
    }
    return injector;
  }
}
