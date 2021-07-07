/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.RE;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.rotation.AdapterTokenVerifier;
import org.keycloak.common.VerificationException;
import org.keycloak.common.util.Time;
import org.keycloak.representations.AccessToken;
import org.keycloak.representations.AccessTokenResponse;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A simple class that manages tokens. Many parts are adopted from org.keycloak.admin.client.token.TokenManager.
 */
public class TokenManager
{
  private final KeycloakDeployment deployment;
  private final TokenService tokenService;

  private final Object tokenLock = new Object();

  /**
   * ID and access tokens are verified if this flag is set. Must be true for production.
   */
  private final boolean verifyToken;

  @GuardedBy("tokenLock")
  @Nullable
  private String accessTokenString;
  @GuardedBy("tokenLock")
  @Nullable
  private String refreshTokenString;

  // this variable is not in use for now, but maybe useful in the future
  // as it contains some information about user identity
  @GuardedBy("tokenLock")
  @Nullable
  private AccessToken accessToken;

  @GuardedBy("tokenLock")
  private long expirationTimeSec;
  @GuardedBy("tokenLock")
  private long refreshTokenExpirationTimeSec;

  public TokenManager(
      KeycloakDeployment deployment,
      Map<String, String> tokenReqHeaders,
      Map<String, String> tokenReqParams
  )
  {
    this(deployment, new TokenService(deployment, tokenReqHeaders, tokenReqParams), true);
  }

  /**
   * Constructor only for testing
   */
  @VisibleForTesting
  public TokenManager(KeycloakDeployment deployment, TokenService service, boolean verifyToken)
  {
    this.deployment = deployment;
    this.tokenService = service;
    this.verifyToken = verifyToken;
  }

  public String getAccessTokenString()
  {
    synchronized (tokenLock) {
      updateTokensIfNeeded();
      return accessTokenString;
    }
  }

  public void updateTokensIfNeeded()
  {
    synchronized (tokenLock) {
      try {
        if (accessTokenString == null || refreshTokenString == null || isRefreshTokenExpired()) {
          grantToken();
        } else if (isTokenExpired() || deployment.isAlwaysRefreshToken()) {
          refreshToken();
        }
      }
      catch (VerificationException e) {
        throw new RE(e, "Failed to verify access token");
      }
    }
  }

  private void grantToken() throws VerificationException
  {
    long requestTime = Time.currentTime();
    AccessTokenResponse tokenResponse = tokenService.grantToken();
    updateTokens(tokenResponse, requestTime);
  }

  private void refreshToken() throws VerificationException
  {
    try {
      long requestTime = Time.currentTime();
      synchronized (tokenLock) {
        AccessTokenResponse tokenResponse = tokenService.refreshToken(refreshTokenString);
        updateTokens(tokenResponse, requestTime);
      }
    }
    catch (KeycloakSecurityBadRequestException e) {
      grantToken();
    }
  }

  private void updateTokens(AccessTokenResponse tokenResponse, long requestTime) throws VerificationException
  {
    synchronized (tokenLock) {
      accessTokenString = tokenResponse.getToken();
      refreshTokenString = tokenResponse.getRefreshToken();
      if (verifyToken) {
        AdapterTokenVerifier.VerifiedTokens parsedTokens = AdapterTokenVerifier.verifyTokens(
            accessTokenString,
            tokenResponse.getIdToken(),
            deployment
        );
        accessToken = parsedTokens.getAccessToken();
      }
      expirationTimeSec = requestTime + tokenResponse.getExpiresIn();
      refreshTokenExpirationTimeSec = requestTime + tokenResponse.getRefreshExpiresIn();
    }
  }

  private boolean isTokenExpired()
  {
    synchronized (tokenLock) {
      return (Time.currentTime() + deployment.getTokenMinimumTimeToLive()) >= expirationTimeSec;
    }
  }

  private boolean isRefreshTokenExpired()
  {
    synchronized (tokenLock) {
      return (Time.currentTime() + deployment.getTokenMinimumTimeToLive()) >= refreshTokenExpirationTimeSec;
    }
  }
}
