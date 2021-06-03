/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.cache;

import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;

import java.util.Map;

/**
 * This class is reponsible for maintaining a cache of the authorization database state.
 * The {@link io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer} uses an injected KeycloakAuthorizerCacheManager
 * to make its authorization decisions.
 */
public interface KeycloakAuthorizerCacheManager
{
  /**
   * Update this cache manager's local state with fresh information pushed by the coordinator.
   * @param serializedRoleMap The updated, serialized role map
   */
  void handleAuthorizerRoleUpdate(byte[] serializedRoleMap);

  /**
   * Return the cache manager's local view of the role map
   *
   * @return Role map
   */
  Map<String, KeycloakAuthorizerRole> getRoleMap();
}
