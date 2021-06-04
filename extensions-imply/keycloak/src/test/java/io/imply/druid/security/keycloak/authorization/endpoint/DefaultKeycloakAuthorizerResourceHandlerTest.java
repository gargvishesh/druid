/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheManager;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;


public class DefaultKeycloakAuthorizerResourceHandlerTest
{
  private KeycloakAuthorizerCacheManager cacheManager;
  private AuthorizerMapper authorizerMapper;

  private DefaultKeycloakAuthorizerResourceHandler target;

  @Before
  public void setup()
  {
    cacheManager = Mockito.mock(KeycloakAuthorizerCacheManager.class);
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of(
        "authorizer", new ImplyKeycloakAuthorizer()
    ));

    target = new DefaultKeycloakAuthorizerResourceHandler(cacheManager, authorizerMapper);
  }

  @Test
  public void test_getAllRoles_NotFound()
  {
    Response response = target.getAllRoles();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_getRole_NotFound()
  {
    Response response = target.getRole("role");
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_createRole_NotFound()
  {
    Response response = target.createRole("role");
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_deleteRole_NotFound()
  {
    Response response = target.deleteRole("role");
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_setRolePermissions_NotFound()
  {
    Response response = target.setRolePermissions("role", ImmutableList.of());
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_getRolePermissions_NotFound()
  {
    Response response = target.getRolePermissions("role");
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_authorizerRoleUpdateListener_keycloakAuthorizerConfigured_ok()
  {
    byte[] roleMapSerialized = StringUtils.toUtf8("ROLE_MAP");
    Response response = target.authorizerRoleUpdateListener(roleMapSerialized);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    Mockito.verify(cacheManager).handleAuthorizerRoleUpdate(roleMapSerialized);
  }

  @Test
  public void test_authorizerRoleUpdateListener_keycloakAuthorizerNotConfigured_badRequest()
  {
    byte[] roleMapSerialized = StringUtils.toUtf8("ROLE_MAP");

    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    target = new DefaultKeycloakAuthorizerResourceHandler(cacheManager, authorizerMapper);
    Response response = target.authorizerRoleUpdateListener(roleMapSerialized);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());

    Mockito.verify(cacheManager, Mockito.never()).handleAuthorizerRoleUpdate(ArgumentMatchers.any());
  }

  @Test
  public void test_refreshAll_NotFound()
  {
    Response response = target.refreshAll();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_getCachedRoleMaps_NotFound()
  {
    Response response = target.getCachedRoleMaps();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
