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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.http.client.AbstractHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;
import org.keycloak.adapters.KeycloakDeployment;

import java.util.HashMap;
import java.util.Map;

public class KeycloakedHttpClient extends AbstractHttpClient
{
  static final String BEARER = "Bearer ";

  private final HttpClient delegate;
  private final TokenManager tokenManager;
  private final boolean isInternal;

  public KeycloakedHttpClient(KeycloakDeployment deployment, HttpClient delegate)
  {
    this(deployment, delegate, true, new HashMap<>(), new HashMap<>());
  }

  /**
   * Constructor only for testing
   */
  @VisibleForTesting
  public KeycloakedHttpClient(
      KeycloakDeployment deployment,
      HttpClient delegate,
      boolean isInternal,
      Map<String, String> tokenReqHeaders,
      Map<String, String> tokenReqParams
  )
  {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      this.isInternal = isInternal;
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      this.tokenManager = new TokenManager(deployment, tokenReqHeaders, tokenReqParams);
      this.tokenManager.updateTokensIfNeeded();
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
    this.delegate = delegate;
  }

  /**
   * Constructor only for testing
   */
  @VisibleForTesting
  public KeycloakedHttpClient(
      HttpClient delegate,
      boolean isInternal,
      TokenManager tokenManager)
  {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      this.isInternal = isInternal;
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      this.tokenManager = tokenManager;
      this.tokenManager.updateTokensIfNeeded();
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
    this.delegate = delegate;
  }

  /**
   * Constructor only for testing
   */
  @VisibleForTesting
  KeycloakedHttpClient(TokenManager tokenManager, HttpClient delegate)
  {
    this.isInternal = true;
    this.tokenManager = tokenManager;
    this.delegate = delegate;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration readTimeout
  )
  {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      request.setHeader(HttpHeaders.Names.AUTHORIZATION, BEARER + tokenManager.getAccessTokenString());
      if (isInternal) {
        request.setHeader(
            DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER,
            DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER_VALUE
        );
      }
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    return delegate.go(request, handler, readTimeout);
  }

  @VisibleForTesting
  public String getAccessTokenString()
  {
    return tokenManager.getAccessTokenString();
  }
}
