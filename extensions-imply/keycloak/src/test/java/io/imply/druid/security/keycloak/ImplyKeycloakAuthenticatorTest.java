/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.representations.adapters.config.AdapterConfig;

public class ImplyKeycloakAuthenticatorTest
{
  private ImplyKeycloakAuthenticator authenticator;

  @Before
  public void setup()
  {
    final AdapterConfig userConfig = new AdapterConfig();
    userConfig.setRealm("realm");
    userConfig.setResource("resource");
    userConfig.setAuthServerUrl("http://url");
    final AdapterConfig internalConfig = new AdapterConfig();
    internalConfig.setRealm("internal-realm");
    internalConfig.setResource("internal-resource");
    internalConfig.setAuthServerUrl("http://internal-url");

    this.authenticator = new ImplyKeycloakAuthenticator(
        "authenticator",
        "authorizer",
        new DruidKeycloakConfigResolver(internalConfig, userConfig)
    );
  }

  @Test
  public void testGetFilter()
  {
    Assert.assertSame(DruidKeycloakOIDCFilter.class, authenticator.getFilterClass());
    final DruidKeycloakOIDCFilter filter = (DruidKeycloakOIDCFilter) authenticator.getFilter();
    Assert.assertEquals("authenticator", filter.getAuthenticatorName());
    Assert.assertEquals("authorizer", filter.getAuthorizerName());
    DruidKeycloakConfigResolver configResolver = (DruidKeycloakConfigResolver) filter.getConfigResolver();
    Assert.assertEquals("realm", configResolver.getUserDeployment().getRealm());
    Assert.assertEquals("resource", configResolver.getUserDeployment().getResourceName());
    Assert.assertEquals("http://url", configResolver.getUserDeployment().getAuthServerBaseUrl());
    Assert.assertEquals("internal-realm", configResolver.getInternalDeployment().getRealm());
    Assert.assertEquals("internal-resource", configResolver.getInternalDeployment().getResourceName());
    Assert.assertEquals("http://internal-url", configResolver.getInternalDeployment().getAuthServerBaseUrl());
  }

  @Test
  public void testGetInitParameters()
  {
    Assert.assertEquals(ImmutableMap.of(), authenticator.getInitParameters());
  }

  @Test
  public void testGetPath()
  {
    Assert.assertEquals("/*", authenticator.getPath());
  }

  @Test
  public void testGetDispatcherType()
  {
    Assert.assertNull(authenticator.getDispatcherType());
  }

  @Test
  public void testGetAuthChallengeHeader()
  {
    Assert.assertEquals("Bearer", authenticator.getAuthChallengeHeader());
  }

  @Test
  public void testAuthenticateJDBCContext()
  {
    Assert.assertNull(authenticator.authenticateJDBCContext(ImmutableMap.of()));
  }
}
