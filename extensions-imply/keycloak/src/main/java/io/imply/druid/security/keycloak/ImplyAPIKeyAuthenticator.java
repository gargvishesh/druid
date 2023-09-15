/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 * Authenticates Imply API keys, a custom API key feature built as a Keycloak extension.
 * <p>
 * This authenticator exchanges the provided API key with a JWT token using {@link ImplyKeycloakAPIKeyAuthHelper}
 * and sets that as the authenticator result. This lets us reuse any existing Keycloak authorizers instead of
 * building a specific one for this.
 * <p>
 * This class differs from {@link ImplyKeycloakAuthenticator} in a few ways
 * - It implements a pass through filter since we do not support API keys for Druid APIs
 * - The basic keycloak authenticator does not generate any tokens while this one done.
 */
@JsonTypeName("imply-keycloak-api-keys")
public class ImplyAPIKeyAuthenticator implements Authenticator
{
  private static final Logger LOG = new Logger(ImplyAPIKeyAuthenticator.class);
  private final DruidKeycloakConfigResolver configResolver;
  private final AccessTokenValidator accessTokenValidator;
  private final ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper;

  @JsonCreator
  public ImplyAPIKeyAuthenticator(
      @JsonProperty("authenticatorName") String authenticatorName,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("scope") String scope,
      @JacksonInject DruidKeycloakConfigResolver configResolver
  )
  {
    Preconditions.checkNotNull(authenticatorName, "authenticatorName");
    Preconditions.checkNotNull(authorizerName, "authorizerName");
    this.configResolver = Preconditions.checkNotNull(configResolver, "configResolver");
    this.accessTokenValidator = new AccessTokenValidator(authenticatorName, authorizerName, configResolver);
    this.apiKeyAuthHelper = new ImplyKeycloakAPIKeyAuthHelper(configResolver, scope);
  }

  @VisibleForTesting
  ImplyAPIKeyAuthenticator(
      DruidKeycloakConfigResolver configResolver,
      AccessTokenValidator accessTokenValidator,
      ImplyKeycloakAPIKeyAuthHelper apiKeyAuthHelper
  )
  {
    this.configResolver = configResolver;
    this.accessTokenValidator = accessTokenValidator;
    this.apiKeyAuthHelper = apiKeyAuthHelper;
  }

  @Nullable
  @Override
  public Filter getFilter()
  {
    // Pass through filter
    return new Filter()
    {
      @Override
      public void init(FilterConfig filterConfig)
      {

      }

      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
          throws IOException, ServletException
      {
        filterChain.doFilter(servletRequest, servletResponse);
      }

      @Override
      public void destroy()
      {

      }
    };
  }

  @Nullable
  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return ImmutableMap.of();
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Nullable
  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Nullable
  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Nullable
  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    String apiKey = (String) context.get("password");
    if (apiKey == null) {
      return null;
    }

    String bearerTokenString = apiKeyAuthHelper.getAccessTokenForAPIKey(apiKey);
    return accessTokenValidator.authenticateToken(
        bearerTokenString,
        configResolver.getUserDeployment()
    );
  }
}
