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
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class KeycloakAuthorizerPermission
{
  private final ResourceAction resourceAction;
  private final Pattern resourceNamePattern;

  @JsonCreator
  public KeycloakAuthorizerPermission(
      @JsonProperty("resourceAction") ResourceAction resourceAction,
      @JsonProperty("resourceNamePattern") Pattern resourceNamePattern
  )
  {
    this.resourceAction = resourceAction;
    this.resourceNamePattern = resourceNamePattern;
  }

  private KeycloakAuthorizerPermission(ResourceAction resourceAction)
  {
    this.resourceAction = resourceAction;
    try {
      this.resourceNamePattern = Pattern.compile(resourceAction.getResource().getName());
    }
    catch (PatternSyntaxException pse) {
      throw new KeycloakSecurityDBResourceException(
          pse,
          "Invalid permission, resource name regex[%s] does not compile.",
          resourceAction.getResource().getName()
      );
    }
  }

  public boolean matches(Resource resource, Action action)
  {
    if (!action.equals(resourceAction.getAction())) {
      return false;
    }

    Resource permissionResource = resourceAction.getResource();
    if (!permissionResource.getType().equals(resource.getType())) {
      return false;
    }

    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }


  @JsonProperty
  public ResourceAction getResourceAction()
  {
    return resourceAction;
  }

  @JsonProperty
  public Pattern getResourceNamePattern()
  {
    return resourceNamePattern;
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

    KeycloakAuthorizerPermission that = (KeycloakAuthorizerPermission) o;

    if (getResourceAction() != null
        ? !getResourceAction().equals(that.getResourceAction())
        : that.getResourceAction() != null) {
      return false;
    }
    return getResourceNamePattern() != null
           ? getResourceNamePattern().pattern().equals(that.getResourceNamePattern().pattern())
           : that.getResourceNamePattern() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getResourceAction() != null ? getResourceAction().hashCode() : 0;
    result = 31 * result + (getResourceNamePattern() != null && getResourceNamePattern().pattern() != null
                            ? getResourceNamePattern().pattern().hashCode()
                            : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "KeycloakAuthorizerPermission{" +
           "resourceAction=" + resourceAction +
           ", resourceNamePattern=" + resourceNamePattern +
           '}';
  }

  public static List<KeycloakAuthorizerPermission> makePermissionList(List<ResourceAction> resourceActions)
  {
    List<KeycloakAuthorizerPermission> permissions = new ArrayList<>();

    if (resourceActions == null) {
      return permissions;
    }

    for (ResourceAction resourceAction : resourceActions) {
      permissions.add(new KeycloakAuthorizerPermission(resourceAction));
    }
    return permissions;
  }
}
