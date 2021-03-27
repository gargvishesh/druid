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
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Pattern;

public class KeycloakAuthorizerPermissionTest
{
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private KeycloakAuthorizerPermission permission;

  @Test
  public void test_serde() throws IOException
  {
    permission = new KeycloakAuthorizerPermission(
        new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ),
        Pattern.compile("resource1")
    );

    byte[] permissionBytes = objectMapper.writeValueAsBytes(permission);
    KeycloakAuthorizerPermission permissionDeserialized = objectMapper.readValue(
        permissionBytes,
        KeycloakAuthorizerPermission.class
    );

    Assert.assertEquals(permission.getResourceAction(), permissionDeserialized.getResourceAction());
    Assert.assertEquals(
        permission.getResourceNamePattern().pattern(),
        permissionDeserialized.getResourceNamePattern().pattern()
    );
    Assert.assertEquals(permission.hashCode(), permissionDeserialized.hashCode());
    Assert.assertEquals(permission.toString(), permissionDeserialized.toString());
    Assert.assertEquals(permission, permissionDeserialized);
  }

  @Test
  public void test_serde_nullFields() throws IOException
  {
    permission = new KeycloakAuthorizerPermission(null, null);

    byte[] permissionBytes = objectMapper.writeValueAsBytes(permission);
    KeycloakAuthorizerPermission permissionDeserialized = objectMapper.readValue(
        permissionBytes,
        KeycloakAuthorizerPermission.class
    );

    Assert.assertEquals(permission.getResourceAction(), permissionDeserialized.getResourceAction());
    Assert.assertEquals(
        permission.getResourceNamePattern(),
        permissionDeserialized.getResourceNamePattern()
    );
    Assert.assertEquals(permission.hashCode(), permissionDeserialized.hashCode());
    Assert.assertEquals(permission.toString(), permissionDeserialized.toString());
    Assert.assertEquals(permission, permissionDeserialized);
  }
}
