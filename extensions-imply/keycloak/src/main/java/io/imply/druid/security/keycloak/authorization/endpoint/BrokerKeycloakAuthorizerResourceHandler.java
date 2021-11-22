/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.google.inject.Inject;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.authorization.state.cache.KeycloakAuthorizerCacheManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrokerKeycloakAuthorizerResourceHandler implements KeycloakAuthorizerResourceHandler
{
  private static final Logger LOG = new Logger(BrokerKeycloakAuthorizerResourceHandler.class);

  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  private final KeycloakAuthorizerCacheManager cache;
  private final Map<String, ImplyKeycloakAuthorizer> authorizerMap;

  @Inject
  public BrokerKeycloakAuthorizerResourceHandler(
      KeycloakAuthorizerCacheManager cacheManager,
      AuthorizerMapper authorizerMapper
  )
  {
    this.cache = cacheManager;

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
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response setRolePermissions(String roleName, List<ResourceAction> permissions)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRolePermissions(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response authorizerRoleUpdateListener(byte[] serializedRoleMap)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      LOG.error("Received update for roles when no keycloak authorizer was found to be configured");
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    cache.updateRoles(serializedRoleMap);
    return Response.ok().build();
  }

  @Override
  public Response authorizerNotBeforeUpdateListener(byte[] serializedNotBeforeMap)
  {
    final ImplyKeycloakAuthorizer authorizer = authorizerMap.get(KeycloakAuthUtils.KEYCLOAK_AUTHORIZER_NAME);
    if (authorizer == null) {
      LOG.error("Received update for roles when no keycloak authorizer was found to be configured");
      return KeycloakCommonErrorResponses.makeResponseForAuthorizerNotFound();
    }

    cache.updateNotBefore(serializedNotBeforeMap);
    return Response.ok().build();
  }

  @Override
  public Response refreshAll()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedRoleMaps()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedNotBeforeMaps()
  {
    return NOT_FOUND_RESPONSE;
  }
}
