/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import io.imply.druid.security.keycloak.authorization.db.cache.CoordinatorKeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheManager;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.authorization.db.cache.MetadataStoragePollingKeycloakAuthorizerCacheManager;
import io.imply.druid.security.keycloak.authorization.db.cache.NoopKeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.authorization.db.updater.CoordinatorKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.db.updater.NoopKeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.endpoint.CoordinatorKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.DefaultKeycloakAuthorizerResourceHandler;
import io.imply.druid.security.keycloak.authorization.endpoint.KeycloakAuthorizerResourceHandler;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleScope;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.mockito.Mockito;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;
import java.util.Set;

public class ImplyKeycloakModuleTest
{
  private AuthorizerMapper authorizerMapper;
  private LifecycleScope lifecycleScope;

  private ImplyKeycloakModule module;

  @Before
  public void setup()
  {
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    lifecycleScope = Mockito.mock(LifecycleScope.class);
    module = new ImplyKeycloakModule();
  }

  @Test
  public void testConfigure()
  {
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final Properties properties = new Properties();
    properties.setProperty("druid.keycloak.realm", "realm");
    properties.setProperty("druid.keycloak.resource", "clientId");
    properties.setProperty("druid.keycloak.auth-server-url", "http://user/auth");

    properties.setProperty("druid.escalator.keycloak.realm", "internal-realm");
    properties.setProperty("druid.escalator.keycloak.resource", "internal-clientId");
    properties.setProperty("druid.escalator.keycloak.auth-server-url", "http://internal/auth");

    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(ObjectMapper.class).toInstance(objectMapper);
          binder.bind(Properties.class).toInstance(properties);
        },
        module
    );
    final AdapterConfig userConfig = injector.getInstance(AdapterConfig.class);
    Assert.assertEquals("realm", userConfig.getRealm());
    Assert.assertEquals("clientId", userConfig.getResource());
    Assert.assertEquals("http://user/auth", userConfig.getAuthServerUrl());

    final AdapterConfig internalConfig = injector.getInstance(Key.get(AdapterConfig.class, EscalatedGlobal.class));
    Assert.assertEquals("internal-realm", internalConfig.getRealm());
    Assert.assertEquals("internal-clientId", internalConfig.getResource());
    Assert.assertEquals("http://internal/auth", internalConfig.getAuthServerUrl());
  }

  @Test
  public void testGetJacksonModule() throws JsonProcessingException
  {
    final AdapterConfig config = new AdapterConfig();
    config.setRealm("realm");
    config.setResource("resource");
    config.setAuthServerUrl("http://url");
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new Std().addValue(
            DruidKeycloakConfigResolver.class,
            new DruidKeycloakConfigResolver(new ImplyKeycloakEscalator("authorizer", config), config))
                 .addValue(AdapterConfig.class, config)
                 .addValue(
                     KeycloakAuthorizerMetadataStorageUpdater.class,
                     new CoordinatorKeycloakAuthorizerMetadataStorageUpdater()
                 )
                 .addValue(KeycloakAuthorizerCacheManager.class, new MetadataStoragePollingKeycloakAuthorizerCacheManager())
                 .addValue(ObjectMapper.class, new ObjectMapper(new SmileFactory()))
    );
    module.getJacksonModules().forEach(mapper::registerModule);
    Assert.assertSame(
        ImplyKeycloakAuthenticator.class,
        mapper.readValue(
            "{\"type\": \"imply-keycloak\", \"authenticatorName\" : \"myAuthenticator\", \"authorizerName\": \"myAuthorizer\", \"rolesTokenClaimName\": \"druid-roles\"}",
            Authenticator.class
        ).getClass()
    );
    Assert.assertSame(
        ImplyKeycloakAuthorizer.class,
        mapper.readValue(
            "{\"type\": \"imply-keycloak\"}",
            Authorizer.class
        ).getClass()
    );
    Assert.assertSame(
        ImplyKeycloakEscalator.class,
        mapper.readValue(
            "{\"type\": \"imply-keycloak\", \"authorizerName\": \"myAuthorizer\"}",
            Escalator.class
        ).getClass()
    );
  }

  @Test
  public void test_getKeycloakAuthorizerResourceHandler_broker_DefaultKeycloakAuthorizerResourceHandler()
  {
    final Injector injector = Guice.createInjector(
        getDependentModules(NodeRole.BROKER).with(
            binder -> {
              binder.bind(KeycloakAuthorizerCacheManager.class)
                    .toInstance(new MetadataStoragePollingKeycloakAuthorizerCacheManager());
            })
    );
    KeycloakAuthorizerResourceHandler authorizerResourceHandler = injector.getInstance(KeycloakAuthorizerResourceHandler.class);
    Assert.assertTrue(authorizerResourceHandler instanceof DefaultKeycloakAuthorizerResourceHandler);
  }

  @Test
  public void test_getKeycloakAuthorizerResourceHandler_coordinator_CoordinatorKeycloakAuthorizerResourceHandler()
  {
    final Injector injector = Guice.createInjector(
        getDependentModules(NodeRole.COORDINATOR).with(
            binder -> {
              binder.bind(KeycloakAuthorizerCacheManager.class)
                    .toInstance(new MetadataStoragePollingKeycloakAuthorizerCacheManager());
              binder.bind(KeycloakAuthorizerMetadataStorageUpdater.class)
                    .toInstance(new NoopKeycloakAuthorizerMetadataStorageUpdater());
              binder.bind(KeycloakAuthCommonCacheConfig.class)
                    .toInstance(new KeycloakAuthCommonCacheConfig());
              binder.bind(KeycloakAuthorizerCacheNotifier.class)
                    .toInstance(new CoordinatorKeycloakAuthorizerCacheNotifier());
            })
    );
    KeycloakAuthorizerResourceHandler authorizerResourceHandler = injector.getInstance(KeycloakAuthorizerResourceHandler.class);
    Assert.assertTrue(authorizerResourceHandler instanceof CoordinatorKeycloakAuthorizerResourceHandler);
  }

  @Test
  public void test_KeycloakAuthorizerMetadataStorageUpdater_broker_CoordinatorKeycloakAuthorizerMetadataStorageUpdater()
  {
    final Properties properties = new Properties();
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bindScope(ManageLifecycle.class, lifecycleScope);
        },
        binder -> {
          binder.bind(
              new TypeLiteral<Set<NodeRole>>()
              {
              }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.BROKER));
        },
        module
    );
    KeycloakAuthorizerMetadataStorageUpdater storageUpdater = injector.getInstance(KeycloakAuthorizerMetadataStorageUpdater.class);
    Assert.assertTrue(storageUpdater instanceof NoopKeycloakAuthorizerMetadataStorageUpdater);
  }

  @Test
  public void test_KeycloakAuthorizerCacheNotifier_broker_NoopKeycloakAuthorizerCacheNotifier()
  {
    final Properties properties = new Properties();
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bindScope(ManageLifecycle.class, lifecycleScope);
        },
        binder -> {
          binder.bind(
              new TypeLiteral<Set<NodeRole>>()
              {
              }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.BROKER));
        },
        module
    );
    KeycloakAuthorizerCacheNotifier cacheNotifier = injector.getInstance(KeycloakAuthorizerCacheNotifier.class);
    Assert.assertTrue(cacheNotifier instanceof NoopKeycloakAuthorizerCacheNotifier);
  }

  Modules.OverriddenModuleBuilder getDependentModules(NodeRole nodeRole)
  {
    final Properties properties = new Properties();
    return Modules.override(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bindScope(ManageLifecycle.class, lifecycleScope);
        },
        binder -> {
          binder.bind(
              new TypeLiteral<Set<NodeRole>>()
              {
              }).annotatedWith(Self.class).toInstance(ImmutableSet.of(nodeRole));
        },
        module
    );
  }
}
