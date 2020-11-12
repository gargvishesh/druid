/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.security;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.AbstractHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.joda.time.Duration;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;

@JsonTypeName("imply")
public class ImplyEscalator implements Escalator
{
  private final String authorizerName;
  private final ImplyTokenValidator tokenValidator;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public ImplyEscalator(
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("secret") String secret
  ) throws NoSuchAlgorithmException, InvalidKeyException
  {
    this.jsonMapper = jsonMapper;
    this.authorizerName = authorizerName;
    this.tokenValidator = new SymmetricTokenValidator(secret);
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return new ImplyHTTPClient(baseClient);
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    final ImplyToken implyToken = ImplyToken.generateInternalClientToken(Long.MAX_VALUE);
    Map<String, Object> context = ImmutableMap.of(
        ImplyAuthenticator.IMPLY_TOKEN_HEADER, implyToken
    );

    // if you found your self asking why the authenticatedBy field is set to null please read this:
    // https://github.com/druid-io/druid/pull/5706#discussion_r185940889
    return new AuthenticationResult(implyToken.getUser(), authorizerName, null, context);
  }

  public class ImplyHTTPClient extends AbstractHttpClient
  {
    private final HttpClient delegate;

    public ImplyHTTPClient(
        HttpClient delegate
    )
    {
      this.delegate = delegate;
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> httpResponseHandler,
        Duration duration
    )
    {
      try {
        Pair<String, String> tokenAndHmac = getEscalatedTokenAndHmac();
        request.setHeader(ImplyAuthenticator.IMPLY_TOKEN_HEADER, tokenAndHmac.lhs);
        request.setHeader(ImplyAuthenticator.IMPLY_HMAC, tokenAndHmac.rhs);
        return this.delegate.go(request, httpResponseHandler, duration);
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private Pair<String, String> getEscalatedTokenAndHmac()
  {
    try {
      final ImplyToken implyToken = ImplyToken.generateInternalClientToken(Long.MAX_VALUE);
      String tokenJson = jsonMapper.writeValueAsString(implyToken);
      String tokenJsonBase64 = Base64.getEncoder().encodeToString(StringUtils.toUtf8(tokenJson));
      return new Pair<>(tokenJsonBase64, tokenValidator.getSignature(tokenJsonBase64));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
