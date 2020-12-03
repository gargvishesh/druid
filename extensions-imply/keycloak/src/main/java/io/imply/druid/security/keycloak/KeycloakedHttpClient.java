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

public class KeycloakedHttpClient extends AbstractHttpClient
{
  static final String BEARER = "Bearer ";

  private final HttpClient delegate;
  private final TokenManager tokenManager;

  public KeycloakedHttpClient(KeycloakDeployment deployment, HttpClient delegate)
  {
    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      this.tokenManager = new TokenManager(deployment);
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
      request.setHeader(
          DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER,
          DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER_VALUE
      );
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    return delegate.go(request, handler, readTimeout);
  }
}
