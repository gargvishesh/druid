/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.cache;

import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * This class is reponsible for maintaining a cache of the authorization role to permission mapping, and 'not-before'
 * policy information for authentication authorization processing.
 *
 * The {@link io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer} uses an injected KeycloakAuthorizerCacheManager
 * to make its authorization decisions.
 */
public interface KeycloakAuthorizerCacheManager
{
  /**
   * Update this cache manager's local state with fresh role information pushed by the coordinator.
   */
  void updateRoles(byte[] serializedRoleMap);

  /**
   * Return the cache manager's local view of the map of role name to {@link KeycloakAuthorizerRole} which contains
   * the set of permissions.
   */
  @Nullable
  Map<String, KeycloakAuthorizerRole> getRoles();

  /**
   * Update this cache manager's local state with fresh revocation information pushed by the coordinator.
   */
  void updateNotBefore(byte[] serializeNotBeforeMap);

  /**
   * Return the cache manager's local view of the map of Keycloak client id to 'not-before' policy timestamp
   * (seconds since epoch). Clients with no 'not-before' policy set will have a value of 0.
   */
  @Nullable
  Map<String, Integer> getNotBefore();

  /**
   * If true, {@link #getNotBefore()} should be used to check that, given the 'azp' of a token, the 'iat' of a token is
   * greater than the value in the cache for that 'azp'.
   */
  boolean validateNotBeforePolicies();
}
