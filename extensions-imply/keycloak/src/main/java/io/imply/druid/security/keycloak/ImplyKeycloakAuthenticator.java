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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("imply-keycloak")
public class ImplyKeycloakAuthenticator implements Authenticator
{
  private final DruidKeycloakConfigResolver configResolver;
  private final String authenticatorName;
  private final String authorizerName;

  @JsonCreator
  public ImplyKeycloakAuthenticator(
      @JsonProperty("authenticatorName") String authenticatorName,
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject DruidKeycloakConfigResolver configResolver
  )
  {
    this.authenticatorName = Preconditions.checkNotNull(authenticatorName, "authenticatorName");
    this.authorizerName = Preconditions.checkNotNull(authorizerName, "authorizerName");
    this.configResolver = Preconditions.checkNotNull(configResolver, "configResolver");
  }

  @Override
  public Filter getFilter()
  {
    return new DruidKeycloakOIDCFilter(configResolver, authenticatorName, authorizerName);
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return DruidKeycloakOIDCFilter.class;
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
    // We support only the "Bearer" header
    return "Bearer";
  }

  @Nullable
  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    // no jdbc
    return null;
  }
}
