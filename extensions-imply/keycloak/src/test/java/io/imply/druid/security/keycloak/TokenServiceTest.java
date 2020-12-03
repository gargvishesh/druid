/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.authentication.ClientIdAndSecretCredentialsProvider;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.util.BasicAuthHelper;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TokenServiceTest
{
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "secret";
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final HttpClient client = Mockito.mock(HttpClient.class);

  private KeycloakDeployment deployment;
  private TokenService tokenService;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    deployment = Mockito.mock(KeycloakDeployment.class);
    Mockito.when(deployment.getResourceName()).thenReturn(CLIENT_ID);
    Mockito.when(deployment.getTokenUrl()).thenReturn("http://localhost:8080/auth/token");
    final ClientIdAndSecretCredentialsProvider credentialsProvider = new ClientIdAndSecretCredentialsProvider();
    credentialsProvider.init(deployment, CLIENT_SECRET);
    Mockito.when(deployment.getClientAuthenticator()).thenReturn(credentialsProvider);
    tokenService = new TokenService(deployment);
  }

  @Test
  public void testGrantTokenVerifyHttpPost() throws IOException
  {
    final AccessTokenResponse expectedResponse = new AccessTokenResponse();
    expectedResponse.setToken("accessToken");
    expectedResponse.setRefreshToken("refreshToken");
    expectedResponse.setExpiresIn(1000);
    expectedResponse.setRefreshExpiresIn(10000);
    mockAccessTokenResponse(expectedResponse);
    final AccessTokenResponse actualResponse = tokenService.grantToken();
    ArgumentCaptor<HttpPost> postCaptor = ArgumentCaptor.forClass(HttpPost.class);
    Mockito.verify(client).execute(postCaptor.capture());

    final HttpPost captured = postCaptor.getValue();
    // verify headers
    final Header[] headers = captured.getAllHeaders();
    Assert.assertEquals(1, headers.length);
    Assert.assertEquals("Authorization", headers[0].getName());
    Assert.assertEquals(BasicAuthHelper.createHeader(CLIENT_ID, CLIENT_SECRET), headers[0].getValue());
    // verify entity
    final HttpEntity capturedEntity = captured.getEntity();
    Assert.assertEquals("Content-Type", capturedEntity.getContentType().getName());
    Assert.assertEquals("application/x-www-form-urlencoded; charset=UTF-8", capturedEntity.getContentType().getValue());
    // verify content
    byte[] buf = new byte[(int) captured.getEntity().getContentLength()];
    Assert.assertEquals(captured.getEntity().getContentLength(), captured.getEntity().getContent().read(buf));
    Assert.assertEquals(
        "grant_type=client_credentials",
        StringUtils.fromUtf8(buf)
    );

    assertEqualsTokens(expectedResponse, actualResponse);
  }

  @Test
  public void testRefreshTokenVerifyHttpPost() throws IOException
  {
    final AccessTokenResponse expectedResponse = new AccessTokenResponse();
    expectedResponse.setToken("newToken");
    expectedResponse.setRefreshToken("refreshToken");
    expectedResponse.setExpiresIn(1000);
    expectedResponse.setRefreshExpiresIn(10000);
    mockAccessTokenResponse(expectedResponse);
    final AccessTokenResponse actualResponse = tokenService.refreshToken("oldToken");
    ArgumentCaptor<HttpPost> postCaptor = ArgumentCaptor.forClass(HttpPost.class);
    Mockito.verify(client).execute(postCaptor.capture());

    final HttpPost captured = postCaptor.getValue();
    // verify headers
    final Header[] headers = captured.getAllHeaders();
    Assert.assertEquals(1, headers.length);
    Assert.assertEquals("Authorization", headers[0].getName());
    Assert.assertEquals(BasicAuthHelper.createHeader(CLIENT_ID, CLIENT_SECRET), headers[0].getValue());
    // verify content type
    final HttpEntity capturedEntity = captured.getEntity();
    Assert.assertEquals("Content-Type", capturedEntity.getContentType().getName());
    Assert.assertEquals("application/x-www-form-urlencoded; charset=UTF-8", capturedEntity.getContentType().getValue());
    // verify content
    byte[] buf = new byte[(int) captured.getEntity().getContentLength()];
    Assert.assertEquals(captured.getEntity().getContentLength(), captured.getEntity().getContent().read(buf));
    Assert.assertEquals(
        "grant_type=client_credentials&grant_type=refresh_token&refresh_token=oldToken",
        StringUtils.fromUtf8(buf)
    );

    assertEqualsTokens(expectedResponse, actualResponse);
  }

  @Test
  public void testRequestTokenNot200Returned() throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500);
    Mockito.when(entity.getContent())
           .thenReturn(new ByteArrayInputStream(StringUtils.toUtf8("failure test")));
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(client.execute(ArgumentMatchers.any())).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Service token grant failed. Bad status: 500 response: failure test");
    tokenService.grantToken();
  }

  @Test
  public void testRequestToken200ButEmptyEntityReturned() throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(null);
    Mockito.when(client.execute(ArgumentMatchers.any())).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("No entity");
    tokenService.grantToken();
  }

  @Test
  public void testRequestTokenIOExceptionThrown() throws IOException
  {
    Mockito.when(client.execute(ArgumentMatchers.any())).thenThrow(new IOException("IOException test"));
    Mockito.when(deployment.getClient()).thenReturn(client);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Service token grant failed. IOException occured. See server.log for details.");
    tokenService.grantToken();
  }

  private void mockAccessTokenResponse(AccessTokenResponse tokenResponse) throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(entity.getContent()).thenReturn(
        new ByteArrayInputStream(jsonMapper.writeValueAsString(tokenResponse).getBytes(StringUtils.UTF8_STRING))
    );
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(client.execute(ArgumentMatchers.any())).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
  }

  private static void assertEqualsTokens(AccessTokenResponse expected, AccessTokenResponse actual)
  {
    Assert.assertEquals(expected.getToken(), actual.getToken());
    Assert.assertEquals(expected.getRefreshExpiresIn(), actual.getRefreshExpiresIn());
    Assert.assertEquals(expected.getExpiresIn(), actual.getExpiresIn());
    Assert.assertEquals(expected.getRefreshExpiresIn(), actual.getRefreshExpiresIn());
  }
}
