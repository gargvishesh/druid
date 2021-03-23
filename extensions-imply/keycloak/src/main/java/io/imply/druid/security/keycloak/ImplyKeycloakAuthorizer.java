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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.guice.annotations.Smile;
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

  private static final List<String> EMPTY_ROLES = ImmutableList.of();
  private final KeycloakAuthorizerMetadataStorageUpdater storageUpdater;
  private final ObjectMapper objectMapper;

  @JsonCreator
  public ImplyKeycloakAuthorizer(
      @JacksonInject KeycloakAuthorizerMetadataStorageUpdater storageUpdater,
      @JacksonInject @Smile ObjectMapper objectMapper

  )
  {
    this.storageUpdater = Preconditions.checkNotNull(storageUpdater, "storageUpdater");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
  }

  @VisibleForTesting
  public ImplyKeycloakAuthorizer()
  {
    this.storageUpdater = null;
    this.objectMapper = null;
  }

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    List<String> allowedRoles = getRolesfromAuthenticationResultContext(authenticationResult);
    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes()
    );
    for (String roleName : allowedRoles) {
      KeycloakAuthorizerRole role = roleMap.get(roleName);
      if (role != null) {
        for (KeycloakAuthorizerPermission permission : role.getPermissions()) {
          if (permissionCheck(resource, action, permission)) {
            return Access.OK;
          }
        }
      }
    }

    return new Access(false);
  }

  private List<String> getRolesfromAuthenticationResultContext(AuthenticationResult authenticationResult)
  {
    Map<String, Object> context = authenticationResult.getContext();
    if (context == null || context.isEmpty())
    {
      LOG.warn("User [%s] has no roles", authenticationResult.getIdentity());
      return EMPTY_ROLES;
    }

    if (context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY) instanceof List) {
      return (List<String>) context.get(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY);
    }

    else {
      LOG.warn("User [%s] roles had unexpected type", authenticationResult.getIdentity());
      return EMPTY_ROLES;
    }
  }

  private boolean permissionCheck(Resource resource, Action action, KeycloakAuthorizerPermission permission)
  {
    if (action != permission.getResourceAction().getAction()) {
      return false;
    }

    Resource permissionResource = permission.getResourceAction().getResource();
    if (permissionResource.getType() != resource.getType()) {
      return false;
    }

    Pattern resourceNamePattern = permission.getResourceNamePattern();
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }
}
