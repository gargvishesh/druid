/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;

public class KeycloakedHttpClientTest
{
  @Test
  public void testGoAttachHeadersProperly() throws MalformedURLException
  {
    final String accessToken = "accessToken";
    final TokenManager tokenManager = Mockito.mock(TokenManager.class);
    Mockito.when(tokenManager.getAccessTokenString()).thenReturn(accessToken);
    final HttpClient delegate = Mockito.mock(HttpClient.class);
    Mockito.when(delegate.go(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenReturn(Futures.immediateFuture(null));

    final KeycloakedHttpClient httpClient = new KeycloakedHttpClient(tokenManager, delegate);
    final Request request = new Request(HttpMethod.GET, new URL("http://sometest.url"));
    httpClient.go(
        request,
        Mockito.mock(HttpResponseHandler.class),
        Duration.millis(1000)
    );

    final Collection<String> authorizationHeaders = request.getHeaders().get(HttpHeaders.AUTHORIZATION);
    Assert.assertEquals(
        KeycloakedHttpClient.BEARER + accessToken,
        Iterables.getOnlyElement(authorizationHeaders)
    );
    final Collection<String> implyInternalRequestHeaders = request.getHeaders().get(
        DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER
    );
    Assert.assertEquals(
        DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER_VALUE,
        Iterables.getOnlyElement(implyInternalRequestHeaders)
    );
  }
}
