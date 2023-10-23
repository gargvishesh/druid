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
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.keycloak.OAuth2Constants;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.authentication.ClientIdAndSecretCredentialsProvider;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.util.BasicAuthHelper;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    tokenService = new TokenService(deployment, new HashMap<>(), new HashMap<>());
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
        StringUtils.format("%s=%s", OAuth2Constants.GRANT_TYPE, OAuth2Constants.CLIENT_CREDENTIALS),
        StringUtils.fromUtf8(buf)
    );

    assertEqualsTokens(expectedResponse, actualResponse);
  }

  @Test
  public void testGetNotBefore() throws IOException
  {
    final AccessTokenResponse someToken = new AccessTokenResponse();
    someToken.setToken("accessToken");
    someToken.setRefreshToken("refreshToken");
    someToken.setExpiresIn(1000);
    someToken.setRefreshExpiresIn(10000);
    tokenService = new TokenService(deployment, new HashMap<>(), ImmutableMap.of("grant_type", "password")) {
      @Override
      protected AccessTokenResponse requestToken(@Nullable String refreshToken)
      {
        return someToken;
      }
    };
    final Map<String, Integer> notBefore = ImmutableMap.of(
        "client1", 0,
        "client2", Ints.checkedCast(DateTimes.nowUtc().plusHours(1).getMillis() / 1000)
    );
    TokenService.ClientTokenNotBeforeResponse expectedResponse = new TokenService.ClientTokenNotBeforeResponse(notBefore);
    mockGetNotBeforeResponse(expectedResponse);
    TokenService.ClientTokenNotBeforeResponse actualResponse = tokenService.getClientNotBefore();

    ArgumentCaptor<HttpGet> getCaptor = ArgumentCaptor.forClass(HttpGet.class);
    Mockito.verify(client, Mockito.times(1)).execute(getCaptor.capture());

    HttpGet captured = getCaptor.getValue();
    final Header[] headers = captured.getAllHeaders();
    Assert.assertEquals("http://localhost:8080/auth/realms/druid/not-before-policies", captured.getURI().toString());
    Assert.assertEquals(1, headers.length);
    Assert.assertEquals("Authorization", headers[0].getName());
    Assert.assertEquals("Bearer accessToken", headers[0].getValue());
    Assert.assertEquals(expectedResponse, actualResponse);
  }

  @Test
  public void testGetNotBefore_retryOnFailure() throws IOException
  {
    final AccessTokenResponse someToken = new AccessTokenResponse();
    someToken.setToken("accessToken");
    someToken.setRefreshToken("refreshToken");
    someToken.setExpiresIn(1000);
    someToken.setRefreshExpiresIn(10000);
    tokenService = new TokenService(deployment, new HashMap<>(), ImmutableMap.of("grant_type", "password")) {
      @Override
      protected AccessTokenResponse requestToken(@Nullable String refreshToken)
      {
        return someToken;
      }
    };
    final Map<String, Integer> notBefore = ImmutableMap.of(
        "client1", 0,
        "client2", Ints.checkedCast(DateTimes.nowUtc().plusHours(1).getMillis() / 1000)
    );
    TokenService.ClientTokenNotBeforeResponse expectedResponse = new TokenService.ClientTokenNotBeforeResponse(notBefore);
    mockGetNotBeforeResponseFailureThenSuccess(expectedResponse);
    TokenService.ClientTokenNotBeforeResponse actualResponse = tokenService.getClientNotBefore();

    ArgumentCaptor<HttpGet> getCaptor = ArgumentCaptor.forClass(HttpGet.class);
    Mockito.verify(client, Mockito.times(3)).execute(getCaptor.capture());

    HttpGet captured = getCaptor.getValue();
    final Header[] headers = captured.getAllHeaders();
    Assert.assertEquals("http://localhost:8080/auth/realms/druid/not-before-policies", captured.getURI().toString());
    Assert.assertEquals(1, headers.length);
    Assert.assertEquals("Authorization", headers[0].getName());
    Assert.assertEquals("Bearer accessToken", headers[0].getValue());
    Assert.assertEquals(expectedResponse, actualResponse);
  }

  @Test
  public void testGetNotBefore_retryOnNoHttpResponse() throws IOException
  {
    final AccessTokenResponse someToken = new AccessTokenResponse();
    someToken.setToken("accessToken");
    someToken.setRefreshToken("refreshToken");
    someToken.setExpiresIn(1000);
    someToken.setRefreshExpiresIn(10000);
    tokenService = new TokenService(deployment, new HashMap<>(), ImmutableMap.of("grant_type", "password")) {
      @Override
      protected AccessTokenResponse requestToken(@Nullable String refreshToken)
      {
        return someToken;
      }
    };
    final Map<String, Integer> notBefore = ImmutableMap.of(
        "client1", 0,
        "client2", Ints.checkedCast(DateTimes.nowUtc().plusHours(1).getMillis() / 1000)
    );
    TokenService.ClientTokenNotBeforeResponse expectedResponse = new TokenService.ClientTokenNotBeforeResponse(notBefore);
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(entity.getContent())
        .thenReturn(new ByteArrayInputStream(jsonMapper.writeValueAsString(expectedResponse).getBytes(StringUtils.UTF8_STRING)));
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);

    Mockito.when(client.execute(ArgumentMatchers.any())).thenThrow(new NoHttpResponseException("Exception test")).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
    tokenService.getClientNotBefore();
    Mockito.verify(client, Mockito.times(2)).execute(ArgumentMatchers.any());



  }

  @Test
  public void testGrantTokenPasswordGrantTypeVerifyHttpPost() throws IOException
  {
    tokenService = new TokenService(deployment, new HashMap<>(), ImmutableMap.of("grant_type", "password"));
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
        StringUtils.format("%s=%s", OAuth2Constants.GRANT_TYPE, OAuth2Constants.PASSWORD),
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
    List<String> pairs = Arrays.stream(
        StringUtils.fromUtf8(buf).split("&")).sorted().collect(Collectors.toList());
    Assert.assertEquals(2, pairs.size());
    Assert.assertEquals("grant_type=refresh_token", pairs.get(0));
    Assert.assertEquals("refresh_token=oldToken", pairs.get(1));
    assertEqualsTokens(expectedResponse, actualResponse);
  }

  @Test
  public void testRequestTokenNot200Returned() throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500);
    Mockito.when(entity.getContent())
        .thenReturn(
           new ByteArrayInputStream(StringUtils.toUtf8("failure test")),
           new ByteArrayInputStream(StringUtils.toUtf8("failure test")),
           new ByteArrayInputStream(StringUtils.toUtf8("failure test"))
      );
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
  public void testRequestTokenSuccessOnRetry() throws IOException
  {

    final AccessTokenResponse expectedResponse = new AccessTokenResponse();
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500, 500, 200);
    Mockito.when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(StringUtils.toUtf8("failure test")),
            new ByteArrayInputStream(StringUtils.toUtf8("failure test")),
            new ByteArrayInputStream(jsonMapper.writeValueAsString(expectedResponse).getBytes(StringUtils.UTF8_STRING))
      );
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(client.execute(ArgumentMatchers.any())).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);

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
  public void testRequestTokenExceptionThrown() throws IOException
  {
    Mockito.when(client.execute(ArgumentMatchers.any())).thenThrow(new IOException("Exception test"));
    Mockito.when(deployment.getClient()).thenReturn(client);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Service token grant failed. Exception occured. See server.log for details.");
    tokenService.grantToken();
  }

  @Test
  public void testRequestTokenNoHTTPResponseExceptionThrown_retries() throws IOException
  {
    final AccessTokenResponse expectedResponse = new AccessTokenResponse();
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(entity.getContent())
        .thenReturn(new ByteArrayInputStream(jsonMapper.writeValueAsString(expectedResponse).getBytes(StringUtils.UTF8_STRING)));
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);

    Mockito.when(client.execute(ArgumentMatchers.any())).thenThrow(new NoHttpResponseException("Exception test")).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
    tokenService.grantToken();
    Mockito.verify(client, Mockito.times(2)).execute(ArgumentMatchers.any());

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
    Mockito.when(client.execute(ArgumentMatchers.argThat(req -> req != null && HttpPost.METHOD_NAME.equals(req.getMethod())))).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
  }

  private void mockGetNotBeforeResponseFailureThenSuccess(TokenService.ClientTokenNotBeforeResponse notBeforeResponse)
      throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(500, 500, 200);
    Mockito.when(entity.getContent()).thenReturn(
        new ByteArrayInputStream("server failure".getBytes(StringUtils.UTF8_STRING)),
        new ByteArrayInputStream("server failure".getBytes(StringUtils.UTF8_STRING)),
        new ByteArrayInputStream(jsonMapper.writeValueAsString(notBeforeResponse).getBytes(StringUtils.UTF8_STRING))
    );
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(client.execute(ArgumentMatchers.argThat(req -> req != null && HttpGet.METHOD_NAME.equals(req.getMethod())))).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
    Mockito.when(deployment.getRealm()).thenReturn("druid");
    Mockito.when(deployment.getAuthServerBaseUrl()).thenReturn("http://localhost:8080/auth");
  }

  private void mockGetNotBeforeResponse(TokenService.ClientTokenNotBeforeResponse notBeforeResponse)
      throws IOException
  {
    final StatusLine statusLine = Mockito.mock(StatusLine.class);
    final HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(entity.getContent()).thenReturn(
        new ByteArrayInputStream(jsonMapper.writeValueAsString(notBeforeResponse).getBytes(StringUtils.UTF8_STRING))
    );
    final HttpResponse response = Mockito.mock(HttpResponse.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(client.execute(ArgumentMatchers.argThat(req -> req != null && HttpGet.METHOD_NAME.equals(req.getMethod())))).thenReturn(response);
    Mockito.when(deployment.getClient()).thenReturn(client);
    Mockito.when(deployment.getRealm()).thenReturn("druid");
    Mockito.when(deployment.getAuthServerBaseUrl()).thenReturn("http://localhost:8080/auth");
  }

  private static void assertEqualsTokens(AccessTokenResponse expected, AccessTokenResponse actual)
  {
    Assert.assertEquals(expected.getToken(), actual.getToken());
    Assert.assertEquals(expected.getRefreshExpiresIn(), actual.getRefreshExpiresIn());
    Assert.assertEquals(expected.getExpiresIn(), actual.getExpiresIn());
    Assert.assertEquals(expected.getRefreshExpiresIn(), actual.getRefreshExpiresIn());
  }
}
