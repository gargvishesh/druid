/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.representations.AccessTokenResponse;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TokenManagerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private TokenService tokenService;
  private TokenManager tokenManager;

  @Before
  public void setup()
  {
    tokenService = mockTokenService();
    tokenManager = new TokenManager(
        mockDevelopment(),
        tokenService,
        false
    );
  }

  @Test
  public void testGrantTokenFailure()
  {
    Mockito.when(tokenService.grantToken()).thenThrow(new RuntimeException("test grant failure"));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("test grant failure");
    tokenManager.getAccessTokenString();
  }

  @Test
  public void testInitialTokenGrant()
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(10000);
    expected.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected);
    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());
  }

  @Test
  public void testGrantTokenInvalidTokenGranted()
  {
    final TokenManager tokenManager = new TokenManager(mockDevelopment(), tokenService, true);
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(10000);
    expected.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Failed to verify access token");
    tokenManager.getAccessTokenString();
  }

  @Test
  public void testGrantTokenReturnCachedTokenBeforeTokenExpires()
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(10000);
    expected.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected);
    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());
    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());
  }

  @Test
  public void testRefreshTokenAfterAccessTokenIsExpired()
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(0);
    expected.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected);
    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());

    final AccessTokenResponse expected2 = new AccessTokenResponse();
    expected2.setToken("accessToken2");
    expected2.setRefreshToken("refreshToken2");
    expected2.setExpiresIn(0);
    expected2.setRefreshExpiresIn(100000);
    mockRefreshToken(tokenService, expected2);
    Assert.assertEquals(expected2.getToken(), tokenManager.getAccessTokenString());
  }

  @Test
  public void testTokenGrantAfterRefreshTokenIsExpired()
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(0);
    expected.setRefreshExpiresIn(0);
    final AccessTokenResponse expected2 = new AccessTokenResponse();
    expected2.setToken("accessToken2");
    expected2.setRefreshToken("refreshToken2");
    expected2.setExpiresIn(0);
    expected2.setRefreshExpiresIn(0);
    mockGrantToken(tokenService, expected, expected2);

    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());
    Assert.assertEquals(expected2.getToken(), tokenManager.getAccessTokenString());
  }

  @Test
  public void testTokenGrantWhenRefreshTokenFailsWithBadRequest()
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(0);
    expected.setRefreshExpiresIn(10000);
    final AccessTokenResponse expected2 = new AccessTokenResponse();
    expected2.setToken("accessToken2");
    expected2.setRefreshToken("refreshToken2");
    expected2.setExpiresIn(10000);
    expected2.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected, expected2);
    Mockito.when(tokenService.refreshToken(ArgumentMatchers.anyString()))
           .thenThrow(new KeycloakSecurityBadRequestException("test"));

    Assert.assertEquals(expected.getToken(), tokenManager.getAccessTokenString());
    Assert.assertEquals(expected2.getToken(), tokenManager.getAccessTokenString());
  }

  @Test
  public void testGrantTokenToMultipleCallersConcurrently()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    final AccessTokenResponse expected = new AccessTokenResponse();
    expected.setToken("accessToken");
    expected.setRefreshToken("refreshToken");
    expected.setExpiresIn(10000);
    expected.setRefreshExpiresIn(100000);
    mockGrantToken(tokenService, expected);

    ExecutorService service = Execs.multiThreaded(4, "token-manager-test-%d");
    try {
      final List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        futures.add(
            service.submit(() -> tokenManager.getAccessTokenString())
        );
      }
      for (Future<String> f : futures) {
        Assert.assertEquals("accessToken", f.get(5, TimeUnit.SECONDS));
      }
    }
    finally {
      service.shutdownNow();
    }
  }

  private static KeycloakDeployment mockDevelopment()
  {
    final KeycloakDeployment deployment = Mockito.mock(KeycloakDeployment.class);
    Mockito.when(deployment.isAlwaysRefreshToken()).thenReturn(false);
    Mockito.when(deployment.getTokenMinimumTimeToLive()).thenReturn(0);
    return deployment;
  }

  private static TokenService mockTokenService()
  {
    return Mockito.mock(TokenService.class);
  }

  private static void mockGrantToken(
      TokenService tokenService,
      AccessTokenResponse tokenResponse,
      AccessTokenResponse... responses
  )
  {
    Mockito.when(tokenService.grantToken()).thenReturn(tokenResponse, responses)
           .thenThrow(new RuntimeException("shouldn't be thrown"));
  }

  private static void mockRefreshToken(TokenService tokenService, AccessTokenResponse tokenResponse)
  {
    Mockito.when(tokenService.refreshToken(ArgumentMatchers.anyString())).thenReturn(tokenResponse)
           .thenThrow(new RuntimeException("shouldn't be thrown"));
  }
}
