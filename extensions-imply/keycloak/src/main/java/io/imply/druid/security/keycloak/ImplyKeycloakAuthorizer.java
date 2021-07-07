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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheManager;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.util.Map;
import java.util.Set;

@JsonTypeName("imply-keycloak")
public class ImplyKeycloakAuthorizer implements Authorizer
{
  private static final Logger LOG = new Logger(ImplyKeycloakAuthorizer.class);

  private final KeycloakAuthorizerCacheManager cacheManager;

  @JsonCreator
  public ImplyKeycloakAuthorizer(
      @JacksonInject KeycloakAuthorizerCacheManager cacheManager
  )
  {
    this.cacheManager = Preconditions.checkNotNull(cacheManager, "cacheManager");
  }

  @VisibleForTesting
  public ImplyKeycloakAuthorizer()
  {
    this.cacheManager = null;
  }

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    if (isSuperuser(authenticationResult)) {
      return Access.OK;
    }

    Set<String> allowedRoles = getRolesfromAuthenticationResultContext(authenticationResult);
    Map<String, KeycloakAuthorizerRole> roleMap = cacheManager.getRoleMap();
    if (roleMap != null) {
      for (String role : allowedRoles) {
        KeycloakAuthorizerRole authorizerRole = roleMap.get(role);
        if (authorizerRole != null) {
          for (KeycloakAuthorizerPermission permission : authorizerRole.getPermissions()) {
            if (permission.matches(resource, action)) {
              return Access.OK;
            }
          }
        } else {
          LOG.warn("Did not find role name [%s] in roleMap", role);
        }
      }
    } else {
      LOG.warn("Rolemap is unavailable, is cache operating?");
    }

    return new Access(false);
  }

  @SuppressWarnings("unchecked")
  private Set<String> getRolesfromAuthenticationResultContext(AuthenticationResult authenticationResult)
  {
    Map<String, Object> context = authenticationResult.getContext();
    if (context == null || context.isEmpty()) {
      LOG.warn("User [%s] has no roles", authenticationResult.getIdentity());
      return KeycloakAuthUtils.EMPTY_ROLES;
    }

    if (context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY) instanceof Set) {
      return (Set<String>) context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY);
    } else {
      LOG.warn("User [%s] roles had unexpected type", authenticationResult.getIdentity());
      return KeycloakAuthUtils.EMPTY_ROLES;
    }
  }

  private boolean isSuperuser(AuthenticationResult authenticationResult)
  {
    Map<String, Object> context = authenticationResult.getContext();
    if (context == null || context.isEmpty()) {
      return false;
    }
    return (boolean) context.getOrDefault(KeycloakAuthUtils.SUPERUSER_CONTEXT_KEY, false);
  }
}
