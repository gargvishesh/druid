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
import java.util.regex.Pattern;

public class KeycloakAuthorizerRoleTest
{
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private KeycloakAuthorizerRole role;

  @Test
  public void test_serde() throws IOException
  {
    role = new KeycloakAuthorizerRole(
        "test_serde",
        ImmutableList.of(
            new KeycloakAuthorizerPermission(
                new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ),
                Pattern.compile("resource1")
            ))
    );
    byte[] roleBytes = objectMapper.writeValueAsBytes(role);
    KeycloakAuthorizerRole roleDeserialized = objectMapper.readValue(
        roleBytes,
        KeycloakAuthorizerRole.class
    );

    Assert.assertEquals(role.getName(), roleDeserialized.getName());
    Assert.assertEquals(role.getPermissions(), roleDeserialized.getPermissions());
    Assert.assertEquals(role.hashCode(), roleDeserialized.hashCode());
    Assert.assertEquals(role, roleDeserialized);
  }

  @Test
  public void test_serde_nullFields() throws IOException
  {
    role = new KeycloakAuthorizerRole(
        null,
        null
    );
    byte[] roleBytes = objectMapper.writeValueAsBytes(role);
    KeycloakAuthorizerRole roleDeserialized = objectMapper.readValue(
        roleBytes,
        KeycloakAuthorizerRole.class
    );

    Assert.assertEquals(role.getName(), roleDeserialized.getName());
    Assert.assertEquals(role.getPermissions(), roleDeserialized.getPermissions());
    Assert.assertEquals(role.hashCode(), roleDeserialized.hashCode());
    Assert.assertEquals(role, roleDeserialized);
  }
}
