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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthenticationResult;
import org.keycloak.adapters.BearerTokenRequestAuthenticator;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.rotation.AdapterTokenVerifier;
import org.keycloak.common.VerificationException;
import org.keycloak.representations.AccessToken;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A class that validates and authenticates {@link AccessToken} retrieved from Keycloak, translating into Druid
 * {@link AuthenticationResult}. Keycloak resource access roles are mapped to the {@link AuthenticationResult#context}
 * keyed as {@link KeycloakAuthUtils#AUTHENTICATED_ROLES_CONTEXT_KEY}. If validating a token for the escalated
 * deployment, an additional key {@link KeycloakAuthUtils#SUPERUSER_CONTEXT_KEY} will be added with the value true,
 * to indicate that this is a superuser token for internal Druid to Druid communication.
 */
public class AccessTokenValidator
{
  private static final Logger LOG = new Logger(AccessTokenValidator.class);

  private final String authenticatorName;
  private final String authorizerName;
  private final DruidKeycloakConfigResolver configResolver;

  public AccessTokenValidator(String authenticatorName, String authorizerName, DruidKeycloakConfigResolver configResolver)
  {
    this.authenticatorName = authenticatorName;
    this.authorizerName = authorizerName;
    this.configResolver = configResolver;
  }

  /**
   * Authenticates the token. This was adapted from {@link BearerTokenRequestAuthenticator#authenticate(org.keycloak.adapters.spi.HttpFacade)}
   *
   * @param tokenString String representation of AccessToken
   * @param deployment The keycloak deployment to use when validating the token.
   * @return If the token is authentic, a {@link AuthenticationResult} is returned. If the token is not authentic,  null is returned.
   */
  @Nullable
  protected AuthenticationResult authenticateToken(String tokenString, KeycloakDeployment deployment)
  {
    AccessToken token;
    try {
      token = verifyToken(tokenString, deployment);
    }
    catch (VerificationException e) {
      LOG.debug("Failed to verify token");
      return null;
    }
    return authenticateToken(token, deployment);
  }

  /**
   * Authenticates the token. This was adapted from {@link BearerTokenRequestAuthenticator#authenticate(org.keycloak.adapters.spi.HttpFacade)}
   *
   * @param token AccessToken
   * @param deployment The keycloak deployment to use when validating the token.
   * @return If the token is authentic, a {@link AuthenticationResult} is returned. If the token is not authentic,  null is returned.
   */
  @Nullable
  protected AuthenticationResult authenticateToken(
      AccessToken token,
      KeycloakDeployment deployment
  )
  {
    if (configResolver.getCacheManager().validateNotBeforePolicies()) {
      if (configResolver.getCacheManager() == null || configResolver.getCacheManager().getNotBefore() == null) {
        LOG.warn("Authentication failed, 'not-before' policies are unavailable, cannot authenticate");
        return null;
      }
      Integer notBefore = configResolver.getCacheManager().getNotBefore().get(token.getIssuedFor());
      if (notBefore != null && token.getIat() < notBefore) {
        LOG.warn(
            "Authentication failed, token for %s issued at %s but must be issued after %s",
            token.getIssuedFor(),
            token.getIat(),
            notBefore
        );
        return null;
      } else if (notBefore == null) {
        LOG.warn("Authentication failed, unable to find token 'not-before' policy for %s", token.getIssuedFor());
        return null;
      }
    }
    LOG.debug("successfully authenticated token, extracting role information");

    Map<String, Object> authContext;
    // always use the base client roles, because the escalated service account assumes roles under the client id
    final String resource = configResolver.getUserDeployment().getResourceName();
    Set<String> roles = Optional.ofNullable(token.getResourceAccess(resource))
                                .map(AccessToken.Access::getRoles)
                                .orElse(KeycloakAuthUtils.EMPTY_ROLES);
    // if deployment is the escalated deployment, also set superuser flag
    if (deployment == configResolver.getInternalDeployment()) {
      authContext = ImmutableMap.of(
          KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles,
          KeycloakAuthUtils.SUPERUSER_CONTEXT_KEY, true
      );
    } else {
      authContext = ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles);
    }
    return new AuthenticationResult(
        token.getPreferredUsername(),
        authorizerName,
        authenticatorName,
        authContext
    );
  }

  @VisibleForTesting
  AccessToken verifyToken(
      String tokenString,
      KeycloakDeployment deployment
  ) throws VerificationException
  {
    return AdapterTokenVerifier.verifyToken(tokenString, deployment);
  }
}
