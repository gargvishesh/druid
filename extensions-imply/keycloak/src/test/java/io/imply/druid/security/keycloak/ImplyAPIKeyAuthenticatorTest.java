/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.authorization.state.cache.KeycloakAuthorizerCacheManager;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.mockito.Mockito;

public class ImplyAPIKeyAuthenticatorTest
{
  private static final String SCOPE = "project-a-cluster-id";
  private ImplyAPIKeyAuthenticator authenticator;

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

    this.authenticator = new ImplyAPIKeyAuthenticator(
        SCOPE,
        "authenticator",
        "authorizer",
        new DruidKeycloakConfigResolver(
            new ImplyKeycloakEscalator("authorizer", internalConfig),
            userConfig,
            Mockito.mock(KeycloakAuthorizerCacheManager.class)
        )
    );
  }

  @Test
  public void test_filter_implementation()
  {
    Assert.assertNull(authenticator.getFilterClass());
    Assert.assertEquals(ImmutableMap.of(), authenticator.getInitParameters());
    Assert.assertEquals("/*", authenticator.getPath());
    Assert.assertNull(authenticator.getDispatcherType());
    Assert.assertNull(authenticator.getAuthChallengeHeader());
  }

  @Test
  public void test_authenticateJDBCContext_noAPIKeyInContext_returnsNull()
  {
    Assert.assertNull(authenticator.authenticateJDBCContext(ImmutableMap.of()));
  }

  @Test
  public void test_authenticateJDBCContext_APIKeyInContext_authenticatesToken()
  {
    AuthenticationResult expectedResult = new AuthenticationResult(
        "identity",
        "authorizer",
        null,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of("role1"))
    );
    String apiKey = "API key";
    String accessTokenStr = "Access Token";
    KeycloakDeployment userDeployment = Mockito.mock(KeycloakDeployment.class);
    DruidKeycloakConfigResolver configResolver = Mockito.mock(DruidKeycloakConfigResolver.class);
    AccessTokenValidator accessTokenValidator = Mockito.mock(AccessTokenValidator.class);
    ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper = Mockito.mock(ImplyKeycloakAPIKeyAuthHelper.class);
    Mockito.when(configResolver.getUserDeployment()).thenReturn(userDeployment);
    Mockito.when(apiKeyAuthHelper.getAccessTokenForAPIKey(apiKey)).thenReturn(accessTokenStr);
    Mockito.when(accessTokenValidator.authenticateToken(accessTokenStr, userDeployment)).thenReturn(expectedResult);
    authenticator = new ImplyAPIKeyAuthenticator(
        configResolver,
        accessTokenValidator,
        apiKeyAuthHelper
    );

    AuthenticationResult actualResult = authenticator.authenticateJDBCContext(ImmutableMap.of("password", apiKey));
    Assert.assertEquals(expectedResult, actualResult);
    Mockito.verify(accessTokenValidator).authenticateToken(accessTokenStr, userDeployment);
  }
}
