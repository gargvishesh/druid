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

import java.util.List;

/**
 * A class that validates and authenticates Access Tokens retrieved from Keycloak
 */
public class AccessTokenValidator
{
  private static final Logger LOG = new Logger(DruidKeycloakConfigResolver.class);

  private final String authorizerName;
  private final String rolesTokenClaimName;

  public AccessTokenValidator(
      String authorizerName,
      String rolesTokenClaimName
  )
  {
    this.authorizerName = authorizerName;
    this.rolesTokenClaimName = rolesTokenClaimName;
  }

  /**
   * Authenticates the token. This was adapted from {@link BearerTokenRequestAuthenticator#authenticate(org.keycloak.adapters.spi.HttpFacade)}
   *
   * @param tokenString String representation of AccessToken
   * @param deployment The keycloak deployment to use when validating the token.
   * @return If the token is authentic, a {@link AuthenticationResult} is returned. If the token is not authentic,  null is returned.
   */
  protected AuthenticationResult authenticateToken(
      String tokenString,
      KeycloakDeployment deployment)
  {
    AccessToken token;
    try {
      token = verifyToken(tokenString, deployment);
    }
    catch (VerificationException e) {
      LOG.debug("Failed to verify token");
      return null;
    }
    if (token.getIat() < deployment.getNotBefore()) {
      LOG.debug("Stale token");
      return null;
    }
    LOG.debug("successful authorized");
    List<Object> implyRoles = getImplyRoles(token);
    return new AuthenticationResult(
        token.getPreferredUsername(),
        authorizerName,
        null,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, implyRoles)
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

  @SuppressWarnings("unchecked")
  private List<Object> getImplyRoles(AccessToken token)
  {
    return (token.getOtherClaims() != null
            && token.getOtherClaims().get(rolesTokenClaimName) != null
            && token.getOtherClaims().get(rolesTokenClaimName) instanceof List) ?
           (List<Object>) token.getOtherClaims().get(rolesTokenClaimName) :
           KeycloakAuthUtils.EMPTY_ROLES;
  }
}
