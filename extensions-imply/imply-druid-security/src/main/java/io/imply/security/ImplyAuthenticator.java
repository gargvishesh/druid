/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.security;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("imply")
public class ImplyAuthenticator implements Authenticator
{
  private static final Logger log = new Logger(ImplyAuthenticator.class);

  private static final Base64.Decoder DECODER = Base64.getDecoder();

  public static final String IMPLY_TOKEN_HEADER = "Imply-Token";
  public static final String IMPLY_HMAC = "Imply-HMAC";

  private final ObjectMapper jsonMapper;
  private final ImplyTokenValidator tokenValidator;
  private final String name;
  private final String authorizerName;

  @JsonCreator
  public ImplyAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("secret") String secret,
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject ObjectMapper jsonMapper
  ) throws NoSuchAlgorithmException, InvalidKeyException
  {
    this.jsonMapper = jsonMapper;
    this.tokenValidator = new SymmetricTokenValidator(secret);
    this.name = Preconditions.checkNotNull(name, "name");
    this.authorizerName = authorizerName;
  }

  @Override
  public Filter getFilter()
  {
    return new ImplyAuthenticationFilter();
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return ImplyAuthenticationFilter.class;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    // Druid expects null to be returned if the authenticator cannot authenticate.
    return null;
  }

  public class ImplyAuthenticationFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig)
    {
      /* Do nothing. */
    }

    @Override
    public void doFilter(
        ServletRequest servletRequest,
        ServletResponse servletResponse,
        FilterChain filterChain
    ) throws IOException, ServletException
    {
      final HttpServletRequest httpReq = (HttpServletRequest) servletRequest;
      final HttpServletResponse httpResp = (HttpServletResponse) servletResponse;

      final String tokenBase64 = httpReq.getHeader(IMPLY_TOKEN_HEADER);
      final String receivedHmac = httpReq.getHeader(IMPLY_HMAC);

      // not an imply format request, move to the next filter
      if (tokenBase64 == null || receivedHmac == null) {
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }

      if (tokenValidator.validate(tokenBase64, receivedHmac)) {
        final String token = StringUtils.fromUtf8(DECODER.decode(tokenBase64));
        final ImplyToken implyToken = jsonMapper.readValue(token, ImplyToken.class);

        Map<String, Object> context = ImmutableMap.of(
            IMPLY_TOKEN_HEADER, implyToken
        );

        AuthenticationResult authenticationResult = new AuthenticationResult(
            implyToken.getUser(),
            authorizerName,
            name,
            context
        );
        servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        filterChain.doFilter(servletRequest, servletResponse);
      } else {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      }
    }

    @Override
    public void destroy()
    {

    }
  }
}
