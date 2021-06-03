/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class KeycloakAuthorizerRoleMapBundle
{
  private final Map<String, KeycloakAuthorizerRole> roleMap;
  private final byte[] serializedRoleMap;

  @JsonCreator
  public KeycloakAuthorizerRoleMapBundle(
      @JsonProperty("roleMap") Map<String, KeycloakAuthorizerRole> roleMap,
      @JsonProperty("serializedRoleMap") byte[] serializedRoleMap
  )
  {
    this.roleMap = roleMap;
    this.serializedRoleMap = serializedRoleMap;
  }

  @JsonProperty
  public Map<String, KeycloakAuthorizerRole> getRoleMap()
  {
    return roleMap;
  }

  @JsonProperty
  public byte[] getSerializedRoleMap()
  {
    return serializedRoleMap;
  }
}
