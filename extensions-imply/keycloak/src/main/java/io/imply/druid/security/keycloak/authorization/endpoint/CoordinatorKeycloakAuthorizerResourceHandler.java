/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleSimplifiedPermissions;
import io.imply.druid.security.keycloak.authorization.state.cache.KeycloakAuthorizerCacheManager;
import io.imply.druid.security.keycloak.authorization.state.updater.KeycloakAuthorizerMetadataStorageUpdater;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorKeycloakAuthorizerResourceHandler implements KeycloakAuthorizerResourceHandler
{
  private final KeycloakAuthorizerMetadataStorageUpdater storageUpdater;
  private final KeycloakAuthorizerCacheManager cacheManager;
  private final Map<String, ImplyKeycloakAuthorizer> authorizerMap;
  private final ObjectMapper objectMapper;

  @Inject
  public CoordinatorKeycloakAuthorizerResourceHandler(
      KeycloakAuthorizerMetadataStorageUpdater storageUpdater,
      KeycloakAuthorizerCacheManager cacheManager,
      AuthorizerMapper authorizerMapper,
      @Smile ObjectMapper objectMapper
  )
  {
    this.storageUpdater = storageUpdater;
    this.cacheManager = cacheManager;
    this.objectMapper = objectMapper;

    this.authorizerMap = new HashMap<>();
    for (Map.Entry<String, Authorizer> authorizerEntry : authorizerMapper.getAuthorizerMap().entrySet()) {
      final Authorizer authorizer = authorizerEntry.getValue();
      if (authorizer instanceof ImplyKeycloakAuthorizer) {
        authorizerMap.put(
            KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME,
            (ImplyKeycloakAuthorizer) authorizer
        );
      }
    }
  }

  @Override
  public Response getAllRoles()
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes()
    );

    return Response.ok(roleMap.keySet()).build();
  }

  @Override
  public Response getRole(String roleName)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes()
    );

    try {
      KeycloakAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
      }
      return Response.ok(new KeycloakAuthorizerRoleSimplifiedPermissions(role)).build();
    }
    catch (KeycloakSecurityDBResourceException e) {
      return makeResponseForKeycloakSecurityDBResourceException(e);
    }
  }

  @Override
  public Response createRole(String roleName)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    try {
      storageUpdater.createRole(roleName);
      return Response.ok().build();
    }
    catch (KeycloakSecurityDBResourceException e) {
      return makeResponseForKeycloakSecurityDBResourceException(e);
    }
  }

  @Override
  public Response deleteRole(String roleName)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    try {
      storageUpdater.deleteRole(roleName);
      return Response.ok().build();
    }
    catch (KeycloakSecurityDBResourceException e) {
      return makeResponseForKeycloakSecurityDBResourceException(e);
    }
  }

  @Override
  public Response setRolePermissions(String roleName, List<ResourceAction> permissions)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    try {
      storageUpdater.setPermissions(roleName, permissions);
      return Response.ok().build();
    }
    catch (KeycloakSecurityDBResourceException e) {
      return makeResponseForKeycloakSecurityDBResourceException(e);
    }
  }

  @Override
  public Response getRolePermissions(String roleName)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes()
    );

    try {
      KeycloakAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
      }
      return Response.ok(role.getPermissions()).build();
    }
    catch (KeycloakSecurityDBResourceException e) {
      return makeResponseForKeycloakSecurityDBResourceException(e);
    }
  }

  @Override
  public Response authorizerRoleUpdateListener(byte[] serializedRoleMap)
  {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Override
  public Response authorizerNotBeforeUpdateListener(byte[] serializedRoleMap)
  {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Override
  public Response refreshAll()
  {
    storageUpdater.refreshAllNotification();
    return Response.ok().build();
  }

  @Override
  public Response getCachedRoleMaps()
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    Map<String, KeycloakAuthorizerRole> roleMap = storageUpdater.getCachedRoleMap();

    return Response.ok(roleMap).build();
  }

  @Override
  public Response getCachedNotBeforeMaps()
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }
    Map<String, Integer> notBefore = cacheManager.getNotBefore();
    return Response.ok(notBefore).build();
  }

  private static Response makeResponseForKeycloakSecurityDBResourceException(KeycloakSecurityDBResourceException e)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error", e.getMessage()
                   ))
                   .build();
  }
}
