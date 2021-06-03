/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleSimplifiedPermissions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CoordinatorKeycloakAuthorizerResourceHandlerTest
{
  private KeycloakAuthorizerMetadataStorageUpdater storageUpdater;
  private AuthorizerMapper authorizerMapper;
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private CoordinatorKeycloakAuthorizerResourceHandler resourceHandler;

  @Before
  public void setup()
  {
    storageUpdater = Mockito.mock(KeycloakAuthorizerMetadataStorageUpdater.class);
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);

    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of(
        "authorizer", new ImplyKeycloakAuthorizer()
    ));

    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );
  }

  @Test
  public void test_getAllRoles_keycloakAuthorizer_succeeds() throws JsonProcessingException
  {
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role1", new KeycloakAuthorizerRole("role1", null),
        "role2", new KeycloakAuthorizerRole("role2", null)
    );
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(objectMapper.writeValueAsBytes(roleMap));

    Response response = resourceHandler.getAllRoles();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableSet.of(
        "role1",
        "role2"
    ), response.getEntity());
  }

  @Test
  public void test_getAllRoles_noKeycloakAuthorizer_fails()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.getAllRoles();

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_getRole_roleExists_succeeds() throws JsonProcessingException
  {
    String roleName = "test_getRole_roleExists_succeeds";
    KeycloakAuthorizerRole role = new KeycloakAuthorizerRole(roleName, ImmutableList.of(
        new KeycloakAuthorizerPermission(
            new ResourceAction(
                new Resource("resource1", ResourceType.DATASOURCE),
                Action.READ
            ),
            Pattern.compile("resource1")
        )));
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(roleName, role);
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(objectMapper.writeValueAsBytes(roleMap));

    Response response = resourceHandler.getRole(roleName);

    KeycloakAuthorizerRoleSimplifiedPermissions expectedRolePermissions =
        new KeycloakAuthorizerRoleSimplifiedPermissions(role);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(expectedRolePermissions, response.getEntity());
  }

  @Test
  public void test_getRole_roleDoesNotExist_fails()
  {
    String roleName = "test_getRole_roleDoesNotExist_fails";
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(null);

    Response response = resourceHandler.getRole(roleName);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        StringUtils.format("Role [%s] does not exist.", roleName)
    ), response.getEntity());
  }

  @Test
  public void test_getRole_noKeycloakAuthorizer_fails()
  {
    String roleName = "test_getRole_roleDoesNotExist_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.getRole(roleName);

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_createRole_succeeds()
  {
    String roleName = "test_createRole_succeeds";

    Response response = resourceHandler.createRole(roleName);

    Mockito.verify(storageUpdater).createRole(roleName);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_createRole_storageUpdaterFails_fails()
  {
    String roleName = "test_createRole_succeeds";
    String errorMessage = "storage updater failed to create role";
    Mockito.doThrow(new KeycloakSecurityDBResourceException(errorMessage)).when(storageUpdater).createRole(roleName);

    Response response = resourceHandler.createRole(roleName);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        errorMessage
    ), response.getEntity());
  }

  @Test
  public void test_createRole_noKeycloakAuthorizer_fails()
  {
    String roleName = "test_createRole_noKeycloakAuthorizer_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.createRole(roleName);

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_deleteRole_succeeds()
  {
    String roleName = "test_deleteRole_succeeds";

    Response response = resourceHandler.deleteRole(roleName);

    Mockito.verify(storageUpdater).deleteRole(roleName);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_deleteRole_storageUpdaterFails_fails()
  {
    String roleName = "test_deleteRole_storageUpdaterFails_fails";
    String errorMessage = "storage updater failed to delete role";
    Mockito.doThrow(new KeycloakSecurityDBResourceException(errorMessage)).when(storageUpdater).deleteRole(roleName);

    Response response = resourceHandler.deleteRole(roleName);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        errorMessage
    ), response.getEntity());
  }

  @Test
  public void test_deleteRole_noKeycloakAuthorizer_fails()
  {
    String roleName = "test_deleteRole_noKeycloakAuthorizer_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.deleteRole(roleName);

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_setRolePermissions_succeeds()
  {
    String roleName = "test_setRolePermissions_succeeds";
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ)
    );

    Response response = resourceHandler.setRolePermissions(roleName, permissions);

    Mockito.verify(storageUpdater).setPermissions(roleName, permissions);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_setRolePermissions_storageUpdaterFails_fails()
  {
    String roleName = "test_setRolePermissions_storageUpdaterFails_fails";
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ)
    );
    String errorMessage = "storage updater failed to set permissions for role";
    Mockito.doThrow(new KeycloakSecurityDBResourceException(errorMessage))
           .when(storageUpdater).setPermissions(roleName, permissions);

    Response response = resourceHandler.setRolePermissions(roleName, permissions);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        errorMessage
    ), response.getEntity());
  }

  @Test
  public void test_setRolePermissions_noKeycloakAuthorizer_fails()
  {
    String roleName = "test_setRolePermissions_noKeycloakAuthorizer_fails";
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource("resource1", ResourceType.DATASOURCE), Action.READ)
    );
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.setRolePermissions(roleName, permissions);

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_getRolePermissions_roleExists_succeeds() throws JsonProcessingException
  {
    String roleName = "test_getRolePermissions_roleExists_succeeds";
    List<KeycloakAuthorizerPermission> rolePermissions = ImmutableList.of(
        new KeycloakAuthorizerPermission(
            new ResourceAction(
                new Resource("resource1", ResourceType.DATASOURCE),
                Action.READ
            ),
            Pattern.compile("resource1")
        ));
    KeycloakAuthorizerRole role = new KeycloakAuthorizerRole(roleName, rolePermissions);
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(roleName, role);
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(objectMapper.writeValueAsBytes(roleMap));


    Response response = resourceHandler.getRolePermissions(roleName);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(rolePermissions, response.getEntity());
  }

  @Test
  public void test_getRolePermissions_roleDoesNotExist_fails() throws JsonProcessingException
  {
    String roleName = "test_getRolePermissions_roleDoesNotExist_fails";
    List<KeycloakAuthorizerPermission> rolePermissions = ImmutableList.of(
        new KeycloakAuthorizerPermission(
            new ResourceAction(
                new Resource("resource1", ResourceType.DATASOURCE),
                Action.READ
            ),
            Pattern.compile("resource1")
        ));
    KeycloakAuthorizerRole role = new KeycloakAuthorizerRole("role1", rolePermissions);
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of("role1", role);
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(objectMapper.writeValueAsBytes(roleMap));


    Response response = resourceHandler.getRolePermissions(roleName);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        StringUtils.format("Role [%s] does not exist.", roleName)
    ), response.getEntity());
  }

  @Test
  public void test_getRolePermissions_noKeycloakAuthorizer_fails()
  {
    String roleName = "test_getRolePermissions_noKeycloakAuthorizer_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.getRolePermissions(roleName);

    Mockito.verify(storageUpdater, Mockito.never()).getCurrentRoleMapBytes();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());
  }

  @Test
  public void test_authorizerRoleUpdateListener_notFound()
  {
    Response response = resourceHandler.authorizerRoleUpdateListener(null);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void test_refreshAll_ok()
  {
    Response response = resourceHandler.refreshAll();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Mockito.verify(storageUpdater).refreshAllNotification();
  }

  @Test
  public void test_getCachedRoleMaps_keycloakAuthorizerConfigured_ok()
  {
    Map<String, KeycloakAuthorizerRole> expectedRoleMap = ImmutableMap.of(
        "role1", new KeycloakAuthorizerRole("role1", null),
        "role2", new KeycloakAuthorizerRole("role2", null)
    );
    Mockito.when(storageUpdater.getCachedRoleMap()).thenReturn(expectedRoleMap);
    Response response = resourceHandler.getCachedRoleMaps();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(expectedRoleMap, response.getEntity());

    Mockito.verify(storageUpdater).getCachedRoleMap();
  }

  @Test
  public void test_getCachedRoleMaps_noKeycloakAuthorizer_fails()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    resourceHandler = new CoordinatorKeycloakAuthorizerResourceHandler(
        storageUpdater,
        authorizerMapper,
        objectMapper
    );

    Response response = resourceHandler.getCachedRoleMaps();

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(
        "error",
        "Keycloak authorizer does not exist."
    ), response.getEntity());

    Mockito.verify(storageUpdater, Mockito.never()).getCachedRoleMap();
  }
}
