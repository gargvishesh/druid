/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Handles authorizer-related API calls. Coordinator and non-coordinator methods are combined here because of an
 * inability to selectively inject jetty resources in configure(Binder binder) of the extension module based
 * on node type.
 */
public interface KeycloakAuthorizerResourceHandler
{
  Response getAllRoles();

  Response getRole(String roleName);

  Response createRole(String roleName);

  Response deleteRole(String roleName);

  Response setRolePermissions(String roleName, List<ResourceAction> permissions);

  Response getRolePermissions(String roleName);

  // non-coordinator methods
  Response authorizerRoleUpdateListener(byte[] serializedRoleMap);

  Response refreshAll();

  Response getCachedRoleMaps();
}
