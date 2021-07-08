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
import com.google.common.collect.Lists;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.List;

/**
 * The POST API for setting permissions on a role represents permissions as ResourceAction options.
 *
 * However, the KeycloakAuthorizerRole object in the metadata store has a different representation for permissions, where
 * the permission object keeps a compiled regex pattern for matching.
 *
 * If we return this role object directly, it is somewhat inconvenient for callers of the API. The returned permissions
 * format does not match the permissions format for update APIs. If the client (e.g. a UI for editing roles) has a
 * workflow where the client retrieves existing roles, edits them, and resubmits for updates, this imposes
 * extra work on the client.
 *
 * This class is used to return role information with permissions represented as ResourceAction objects to match
 * the update APIs.
 *
 * The compiled regex pattern is not useful for users, so the response formats that contain the permission+regex are
 * now deprecated. In the future user-facing APIs should only return role information with the simplfied permissions
 * format.
 */
public class KeycloakAuthorizerRoleSimplifiedPermissions
{
  private final String name;
  private final List<ResourceAction> permissions;

  @JsonCreator
  public KeycloakAuthorizerRoleSimplifiedPermissions(
      @JsonProperty("name") String name,
      @JsonProperty("permissions") List<ResourceAction> permissions
  )
  {
    this.name = name;
    this.permissions = permissions == null ? new ArrayList<>() : permissions;
  }

  public KeycloakAuthorizerRoleSimplifiedPermissions(KeycloakAuthorizerRole role)
  {
    this(role.getName(), convertPermissions(role.getPermissions()));
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<ResourceAction> getPermissions()
  {
    return permissions;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KeycloakAuthorizerRoleSimplifiedPermissions that = (KeycloakAuthorizerRoleSimplifiedPermissions) o;

    if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(that.getPermissions()) : that.getPermissions() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }

  public static List<ResourceAction> convertPermissions(
      List<KeycloakAuthorizerPermission> permissions
  )
  {
    return Lists.transform(permissions, KeycloakAuthorizerPermission::getResourceAction);
  }
}
