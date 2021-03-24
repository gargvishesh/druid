/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KeycloakAuthorizerRoleSimplifiedPermissionsTest
{
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private KeycloakAuthorizerRoleSimplifiedPermissions permissions;

  @Test
  public void test_serde() throws IOException
  {
    permissions = new KeycloakAuthorizerRoleSimplifiedPermissions(
        "test_serde",
        ImmutableList.of(
            new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ)
        )
    );

    byte[] permissionsBytes = objectMapper.writeValueAsBytes(permissions);
    KeycloakAuthorizerRoleSimplifiedPermissions permissionsDeserialized = objectMapper.readValue(
        permissionsBytes,
        KeycloakAuthorizerRoleSimplifiedPermissions.class
    );

    Assert.assertEquals(permissions.getName(), permissionsDeserialized.getName());
    Assert.assertEquals(permissions.getPermissions(), permissionsDeserialized.getPermissions());
    Assert.assertEquals(permissions.hashCode(), permissionsDeserialized.hashCode());
    Assert.assertEquals(permissions, permissionsDeserialized);
  }

  @Test
  public void test_serde_nullFields() throws IOException
  {
    permissions = new KeycloakAuthorizerRoleSimplifiedPermissions(null, null);

    byte[] permissionsBytes = objectMapper.writeValueAsBytes(permissions);
    KeycloakAuthorizerRoleSimplifiedPermissions permissionsDeserialized = objectMapper.readValue(
        permissionsBytes,
        KeycloakAuthorizerRoleSimplifiedPermissions.class
    );

    Assert.assertEquals(permissions.getName(), permissionsDeserialized.getName());
    Assert.assertEquals(permissions.getPermissions(), permissionsDeserialized.getPermissions());
    Assert.assertEquals(permissions.hashCode(), permissionsDeserialized.hashCode());
    Assert.assertEquals(permissions, permissionsDeserialized);
  }
}
