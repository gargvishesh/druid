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
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheManager;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("imply-keycloak")
public class ImplyKeycloakAuthorizer implements Authorizer
{
  private static final Logger LOG = new Logger(ImplyKeycloakAuthorizer.class);

  private final KeycloakAuthorizerCacheManager cacheManager;

  @JsonCreator
  public ImplyKeycloakAuthorizer(
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("test_setUpdateSource_cacheNotifictionsEnabled_sendUpdate") Long notifierUpdatePeriod,
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
    List<Object> allowedRoles = getRolesfromAuthenticationResultContext(authenticationResult);
    Map<String, KeycloakAuthorizerRole> roleMap = cacheManager.getRoleMap();
    for (Object role : allowedRoles) {
      String roleName;
      try {
        roleName = (String) role;
        KeycloakAuthorizerRole authorizerRole = roleMap == null ? null : roleMap.get(roleName);
        if (authorizerRole != null) {
          for (KeycloakAuthorizerPermission permission : authorizerRole.getPermissions()) {
            if (permissionCheck(resource, action, permission)) {
              return Access.OK;
            }
          }
        } else {
          LOG.warn("Did not found role name [%s] in roleMap", roleName);
        }
      }
      catch (ClassCastException e) {
        LOG.warn("Could not cast role [%s] to string format", role);
      }
    }

    return new Access(false);
  }

  @SuppressWarnings("unchecked")
  private List<Object> getRolesfromAuthenticationResultContext(AuthenticationResult authenticationResult)
  {
    Map<String, Object> context = authenticationResult.getContext();
    if (context == null || context.isEmpty()) {
      LOG.warn("User [%s] has no roles", authenticationResult.getIdentity());
      return KeycloakAuthUtils.EMPTY_ROLES;
    }

    if (context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY) instanceof List) {
      return (List<Object>) context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY);
    } else {
      LOG.warn("User [%s] roles had unexpected type", authenticationResult.getIdentity());
      return KeycloakAuthUtils.EMPTY_ROLES;
    }
  }

  private boolean permissionCheck(Resource resource, Action action, KeycloakAuthorizerPermission permission)
  {
    if (!action.equals(permission.getResourceAction().getAction())) {
      return false;
    }

    Resource permissionResource = permission.getResourceAction().getResource();
    if (!permissionResource.getType().equals(resource.getType())) {
      return false;
    }

    Pattern resourceNamePattern = permission.getResourceNamePattern();
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }
}
