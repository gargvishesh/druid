/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeycloakAuthUtils
{
  public static final String ADMIN_NAME = "admin";
  public static final String KEYCLOAK_AUTHORIZER_NAME = "keycloak-authorizer";
  public static final String AUTHENTICATED_ROLES_CONTEXT_KEY = "imply-roles";
  public static final List<Object> EMPTY_ROLES = ImmutableList.of();

  public static final Predicate<Throwable> SHOULD_RETRY_INIT =
      (throwable) -> throwable instanceof KeycloakSecurityDBResourceException;

  public static final int MAX_INIT_RETRIES = 2;

  public static final TypeReference<Map<String, KeycloakAuthorizerRole>> AUTHORIZER_ROLE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, KeycloakAuthorizerRole>>()
      {
      };

  public static final Map<String, Object> CONTEXT_WITH_ADMIN_ROLE = ImmutableMap.of(
      KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of(ADMIN_NAME)
  );

  public static final String ROLE_MAP_CACHE_KEY = "keycloak-role-map-cache-key";

  public static Map<String, KeycloakAuthorizerRole> deserializeAuthorizerRoleMap(
      ObjectMapper objectMapper,
      byte[] roleMapBytes
  )
  {
    Map<String, KeycloakAuthorizerRole> roleMap;
    if (roleMapBytes == null) {
      roleMap = new HashMap<>();
    } else {
      try {
        roleMap = objectMapper.readValue(roleMapBytes, KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException("Couldn't deserialize authorizer roleMap!", ioe);
      }
    }
    return roleMap;
  }

  public static byte[] serializeAuthorizerRoleMap(ObjectMapper objectMapper, Map<String, KeycloakAuthorizerRole> roleMap)
  {
    try {
      return objectMapper.writeValueAsBytes(roleMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "Couldn't serialize authorizer roleMap!");
    }
  }

  public static void maybeInitialize(final RetryUtils.Task<?> task)
  {
    try {
      RetryUtils.retry(task, SHOULD_RETRY_INIT, MAX_INIT_RETRIES);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
