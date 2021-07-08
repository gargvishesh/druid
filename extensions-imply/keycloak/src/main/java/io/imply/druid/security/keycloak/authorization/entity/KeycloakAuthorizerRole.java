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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeycloakAuthorizerRole
{
  private static final Logger log = new Logger(KeycloakAuthorizerRole.class);

  private final String name;
  private final List<KeycloakAuthorizerPermission> permissions;

  @JsonCreator
  public KeycloakAuthorizerRole(
      @JsonProperty("name") String name,
      @JsonProperty("permissions") @JsonDeserialize(using = PermissionsDeserializer.class) List<KeycloakAuthorizerPermission> permissions
  )
  {
    this.name = name;
    this.permissions = permissions == null ? new ArrayList<>() : permissions;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<KeycloakAuthorizerPermission> getPermissions()
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

    KeycloakAuthorizerRole role = (KeycloakAuthorizerRole) o;

    if (getName() != null ? !getName().equals(role.getName()) : role.getName() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(role.getPermissions()) : role.getPermissions() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "KeycloakAuthorizerRole{" +
           "name=" + name +
           ", permissions=" + permissions +
           '}';
  }

  static class PermissionsDeserializer extends JsonDeserializer<List<KeycloakAuthorizerPermission>>
  {
    @Override
    public List<KeycloakAuthorizerPermission> deserialize(
        JsonParser jsonParser,
        DeserializationContext deserializationContext
    ) throws IOException
    {
      List<KeycloakAuthorizerPermission> permissions = new ArrayList<>();
      // sanity check
      ObjectCodec codec = jsonParser.getCodec();
      JsonNode hopefullyAnArray = codec.readTree(jsonParser);
      if (!hopefullyAnArray.isArray()) {
        throw new RE("Failed to deserialize authorizer role list");
      }

      for (JsonNode node : hopefullyAnArray) {
        try {
          permissions.add(codec.treeToValue(node, KeycloakAuthorizerPermission.class));
        }
        catch (JsonProcessingException e) {
          // ignore unparseable, it might be resource types we don't know about
          log.warn(e, "Failed to deserialize authorizer role, ignoring: %s", node.toPrettyString());
        }
      }

      return permissions;
    }
  }
}
