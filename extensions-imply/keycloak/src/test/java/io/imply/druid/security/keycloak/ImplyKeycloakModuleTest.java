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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Escalator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.representations.adapters.config.AdapterConfig;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class ImplyKeycloakModuleTest
{
  private ImplyKeycloakModule module;

  @Before
  public void setup()
  {
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
        new Std().addValue(DruidKeycloakConfigResolver.class, new DruidKeycloakConfigResolver(config, config))
                 .addValue(AdapterConfig.class, config)
    );
    module.getJacksonModules().forEach(mapper::registerModule);
    Assert.assertSame(
        ImplyKeycloakAuthenticator.class,
        mapper.readValue(
            "{\"type\": \"imply-keycloak\", \"authenticatorName\" : \"myAuthenticator\", \"authorizerName\": \"myAuthorizer\"}",
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
}
