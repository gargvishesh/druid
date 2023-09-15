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
import org.keycloak.adapters.KeycloakDeployment;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImplyKeycloakAPIKeyAuthHelperTest
{
  private static final String SCOPE = "project-a-cluster-id";
  private DruidKeycloakConfigResolver resolver;
  private KeycloakDeployment userDeployment;

  @Before
  public void setup()
  {
    this.resolver = Mockito.mock(DruidKeycloakConfigResolver.class);
    this.userDeployment = Mockito.mock(KeycloakDeployment.class);
  }

  @Test
  public void test_getAccessTokenForAPIKey_succeeds_once()
  {
    ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper = new ImplyKeycloakAPIKeyAuthHelper(
        resolver,
        "some_client_id",
        SCOPE
    );

    String validApiKey = "pol_ZRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    try (MockedStatic<ImplyKeycloakAPIKeyAuthHelper> staticMock = Mockito.mockStatic(
        ImplyKeycloakAPIKeyAuthHelper.class)) {
      TokenManager tokenManager = Mockito.mock(TokenManager.class);
      staticMock.when(() -> ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey,
          SCOPE
      )).thenReturn(tokenManager);
      Mockito.when(resolver.getUserDeployment()).thenReturn(userDeployment);
      Mockito.when(tokenManager.getAccessTokenString()).thenReturn("some_token");

      Assert.assertEquals(
          "some_token",
          apiKeyAuthHelper.getAccessTokenForAPIKey(validApiKey)
      );
    }
  }

  @Test
  public void test_getAccessTokenForAPIKey_fetch_from_cache()
  {
    ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper = new ImplyKeycloakAPIKeyAuthHelper(
        resolver,
        "some_client_id",
        SCOPE
    );

    String validApiKey = "pol_ZRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    try (MockedStatic<ImplyKeycloakAPIKeyAuthHelper> staticMock = Mockito.mockStatic(
        ImplyKeycloakAPIKeyAuthHelper.class)) {
      TokenManager tokenManager = Mockito.mock(TokenManager.class);
      staticMock.when(() -> ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey,
          SCOPE
      )).thenReturn(tokenManager);
      Mockito.when(resolver.getUserDeployment()).thenReturn(userDeployment);
      Mockito.when(tokenManager.getAccessTokenString()).thenReturn("some_token");

      Assert.assertEquals(
          "some_token",
          apiKeyAuthHelper.getAccessTokenForAPIKey(validApiKey)
      );
      Assert.assertEquals(
          "some_token",
          apiKeyAuthHelper.getAccessTokenForAPIKey(validApiKey)
      );
    }
  }

  @Test
  public void test_getAccessTokenForAPIKey_succeeds_twice()
  {
    ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper = new ImplyKeycloakAPIKeyAuthHelper(
        resolver,
        "some_client_id",
        SCOPE
    );

    String validApiKey1 = "pol_ZRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    String validApiKey2 = "pol_YRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    try (MockedStatic<ImplyKeycloakAPIKeyAuthHelper> staticMock = Mockito.mockStatic(
        ImplyKeycloakAPIKeyAuthHelper.class)) {
      TokenManager tokenManager1 = Mockito.mock(TokenManager.class);
      TokenManager tokenManager2 = Mockito.mock(TokenManager.class);
      staticMock.when(() -> ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey1,
          SCOPE
      )).thenReturn(tokenManager1);
      staticMock.when(() -> ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey2,
          SCOPE
      )).thenReturn(tokenManager2);
      Mockito.when(resolver.getUserDeployment()).thenReturn(userDeployment);
      Mockito.when(tokenManager1.getAccessTokenString()).thenReturn("some_token1");
      Mockito.when(tokenManager2.getAccessTokenString()).thenReturn("some_token2");

      Assert.assertEquals(
          "some_token1",
          apiKeyAuthHelper.getAccessTokenForAPIKey(validApiKey1)
      );
      Assert.assertEquals(
          "some_token2",
          apiKeyAuthHelper.getAccessTokenForAPIKey(validApiKey2)
      );
    }
  }

  @Test
  public void test_newTokenManager_withScope()
  {
    final Map<TokenManager, List<Object>> constructorArgs = new HashMap<>();
    String validApiKey = "pol_ZRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    try (MockedConstruction<TokenManager> mockedClass = Mockito.mockConstruction(
        TokenManager.class,
        (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments()))
    )) {
      TokenManager tokenManager = ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey,
          SCOPE
      );

      List<TokenManager> constructed = mockedClass.constructed();
      Assert.assertEquals(1, constructed.size());
      Assert.assertEquals(3, constructorArgs.get(tokenManager).size());
      Assert.assertEquals(userDeployment, constructorArgs.get(tokenManager).get(0));
      Assert.assertEquals(Collections.emptyMap(), constructorArgs.get(tokenManager).get(1));
      Assert.assertEquals(
          ImmutableMap.of(
              "grant_type", "password",
              "client_id", "some_client_id",
              "apiKey", validApiKey,
              "scope", SCOPE
          ),
          constructorArgs.get(tokenManager).get(2)
      );
    }
  }

  @Test
  public void test_newTokenManager_withoutScope()
  {
    final Map<TokenManager, List<Object>> constructorArgs = new HashMap<>();
    String validApiKey = "pol_ZRcifWAC5nfeUWn0ai336y5vkhOzzCY4l359FYz5OUD17T7JmFeNpdtfrXL6my46HN";
    try (MockedConstruction<TokenManager> mockedClass = Mockito.mockConstruction(
        TokenManager.class,
        (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments()))
    )) {
      TokenManager tokenManager = ImplyKeycloakAPIKeyAuthHelper.newTokenManager(
          this.userDeployment,
          "some_client_id",
          validApiKey,
          null
      );

      List<TokenManager> constructed = mockedClass.constructed();
      Assert.assertEquals(1, constructed.size());
      Assert.assertEquals(3, constructorArgs.get(tokenManager).size());
      Assert.assertEquals(userDeployment, constructorArgs.get(tokenManager).get(0));
      Assert.assertEquals(Collections.emptyMap(), constructorArgs.get(tokenManager).get(1));
      Assert.assertEquals(
          ImmutableMap.of(
              "grant_type", "password",
              "client_id", "some_client_id",
              "apiKey", validApiKey
          ),
          constructorArgs.get(tokenManager).get(2)
      );
    }
  }
}
