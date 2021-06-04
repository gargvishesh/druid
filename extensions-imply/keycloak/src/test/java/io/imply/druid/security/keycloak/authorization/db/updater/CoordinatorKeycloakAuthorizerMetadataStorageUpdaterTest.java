/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.updater;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheNotifier;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleMapBundle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class CoordinatorKeycloakAuthorizerMetadataStorageUpdaterTest
{
  private static final Map<String, Authorizer> AUTHORIZER_MAP_WITH_KEYCLOAK =
      ImmutableMap.of("keycloak", new ImplyKeycloakAuthorizer());

  private static final List<KeycloakAuthorizerPermission> SUPER_PERMISSIONS = ImmutableList.of(
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.DATASOURCE), Action.READ),
          Pattern.compile(".*")
      ),
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.DATASOURCE), Action.WRITE),
          Pattern.compile(".*")
      ),
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.CONFIG), Action.READ),
          Pattern.compile(".*")
      ),
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.CONFIG), Action.WRITE),
          Pattern.compile(".*")
      ),
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.STATE), Action.READ),
          Pattern.compile(".*")
      ),
      new KeycloakAuthorizerPermission(
          new ResourceAction(new Resource(".*", ResourceType.STATE), Action.WRITE),
          Pattern.compile(".*")
      )
  );

  private AuthorizerMapper authorizerMapper;
  private MetadataStorageConnector connector;
  private MetadataStorageTablesConfig connectorConfig;
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());
  private KeycloakAuthorizerCacheNotifier cacheNotifier;
  private KeycloakAuthCommonCacheConfig commonCacheConfig;

  private CoordinatorKeycloakAuthorizerMetadataStorageUpdater storageUpdater;

  @SuppressWarnings("unchecked")
  ArgumentCaptor<Supplier<byte[]>> roleMapByteSupplierCaptor = ArgumentCaptor.forClass((Class) Supplier.class);

  @Before
  public void setup()
  {
    Injector injector = Mockito.mock(Injector.class);
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    connector = Mockito.mock(MetadataStorageConnector.class);
    connectorConfig = Mockito.mock(MetadataStorageTablesConfig.class);
    cacheNotifier = Mockito.mock(KeycloakAuthorizerCacheNotifier.class);
    commonCacheConfig = Mockito.mock(KeycloakAuthCommonCacheConfig.class);
    Mockito.when(injector.getInstance(AuthorizerMapper.class)).thenReturn(authorizerMapper);
    Mockito.when(connectorConfig.getConfigTable()).thenReturn("config");
    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(200_000_000L);

    storageUpdater = new CoordinatorKeycloakAuthorizerMetadataStorageUpdater(
        injector,
        connector,
        connectorConfig,
        objectMapper,
        cacheNotifier,
        commonCacheConfig
    );
  }

  @Test(expected = ISE.class)
  public void test_start_twoTimes_throwsException()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    storageUpdater.start();
    storageUpdater.start();
  }

  @Test
  public void test_start_noKeycloakAuthorizer_doesNotInitializeAdminRole()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    storageUpdater.start();
    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(connector, Mockito.never()).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString()
    );
  }

  @Test
  public void test_start_keycloakAuthorizerAndAdminRoleNotInitialized_initializesAdminRole()
      throws JsonProcessingException
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(AUTHORIZER_MAP_WITH_KEYCLOAK);
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    ))
           .thenReturn(null)
           .thenReturn(null)
           .thenReturn(null)
           .thenReturn(objectMapper.writeValueAsBytes(ImmutableMap.of(
               "admin", new KeycloakAuthorizerRole("admin", null))));

    Mockito.when(connector.compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
        arg,
        "admin",
        ImmutableList.of()
    ))))
           .thenReturn(false)
           .thenReturn(true);

    Mockito.when(connector.compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
        arg,
        "admin",
        SUPER_PERMISSIONS
    ))))
           .thenReturn(true);

    Mockito.when(commonCacheConfig.getPollingPeriod()).thenReturn(0L);

    storageUpdater.start();
    storageUpdater.stop();

    Mockito.verify(connector, Mockito.times(2))
           .compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
               arg,
               "admin",
               ImmutableList.of()
           )));
    Mockito.verify(connector)
           .compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
               arg,
               "admin",
               SUPER_PERMISSIONS
           )));
  }

  @Test
  public void test_start_keycloakAuthorizerAndAdminRoleInitialized_doesNotinitializeAdminRole()
      throws JsonProcessingException
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    Mockito.when(connectorConfig.getConfigTable()).thenReturn("config");
    byte[] adminRoleBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "admin", new KeycloakAuthorizerRole("admin", null)));
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(adminRoleBytes);
    storageUpdater.start();
    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).setUpdateSource(ArgumentMatchers.any());
  }

  @Test(expected = IllegalMonitorStateException.class)
  public void test_stop_beforeStart_throwsException()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    storageUpdater.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void test_createRole_notStarted_fails()
  {
    storageUpdater.createRole("role1");
    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).setUpdateSource(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).scheduleUpdate();
  }

  @Test
  public void test_createRole_newRole_succeeds() throws IOException
  {
    String roleName = "test_createRole_newRole_succeeds";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(null);

    Mockito.when(connector.compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
        arg,
        roleName,
        ImmutableList.of()
    ))))
           .thenReturn(true);

    Map<String, KeycloakAuthorizerRole> expectedRoleMap = ImmutableMap.of(
        roleName, new KeycloakAuthorizerRole(roleName, null));
    byte[] expectedRoleMapBytes = objectMapper.writeValueAsBytes(expectedRoleMap);
    KeycloakAuthorizerRoleMapBundle expectedRoleMapBundle = new KeycloakAuthorizerRoleMapBundle(
        expectedRoleMap,
        expectedRoleMapBytes
    );

    storageUpdater.start();
    storageUpdater.createRole(roleName);
    Assert.assertEquals(expectedRoleMap, storageUpdater.getCachedRoleMap());

    Mockito.verify(connector)
           .compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
               arg,
               roleName,
               ImmutableList.of()
           )));
    Mockito.verify(cacheNotifier).scheduleUpdate();
  }

  @Test(expected = KeycloakSecurityDBResourceException.class)
  public void test_createRole_roleExists_fails() throws JsonProcessingException
  {
    String roleName = "test_createRole_roleExists_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    byte[] roleBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        roleName, new KeycloakAuthorizerRole(roleName, null)));
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(roleBytes);

    storageUpdater.start();
    storageUpdater.createRole(roleName);

    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).setUpdateSource(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).scheduleUpdate();
  }

  @Test
  public void test_deleteRole_roleExists_succeeds() throws IOException
  {
    String roleName = "test_deleteRole_roleExists_succeeds";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    byte[] roleBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        roleName, new KeycloakAuthorizerRole(roleName, null)));
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(roleBytes);

    Mockito.when(connector.compareAndSwap(ArgumentMatchers.argThat((arg) -> roleDeleted(
        arg,
        roleName
    ))))
           .thenReturn(true);

    Map<String, KeycloakAuthorizerRole> expectedRoleMap = ImmutableMap.of();

    storageUpdater.start();
    storageUpdater.deleteRole(roleName);
    Assert.assertEquals(expectedRoleMap, storageUpdater.getCachedRoleMap());

    Mockito.verify(connector)
           .compareAndSwap(ArgumentMatchers.argThat((arg) -> roleDeleted(
               arg,
               roleName
           )));
    Mockito.verify(cacheNotifier).scheduleUpdate();
  }

  @Test(expected = KeycloakSecurityDBResourceException.class)
  public void test_deleteRole_roleDoesNotExist_fails()
  {
    String roleName = "test_deleteRole_roleDoesNotExist_fails";
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(null);

    storageUpdater.start();
    storageUpdater.deleteRole(roleName);

    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).setUpdateSource(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).scheduleUpdate();
  }

  @Test
  public void test_setPermissions_roleExists_succeeds() throws IOException
  {
    String roleName = "test_setPermissions_roleExists_succeeds";
    ResourceAction resourceAction = new ResourceAction(
        new Resource("datasourc1", ResourceType.DATASOURCE),
        Action.READ
    );
    List<ResourceAction> updatedPermissions = ImmutableList.of(
        resourceAction
    );
    List<KeycloakAuthorizerPermission> updatedAuthorizerPermissions =
        KeycloakAuthorizerPermission.makePermissionList(updatedPermissions);
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    byte[] roleBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        roleName, new KeycloakAuthorizerRole(roleName, null)));
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(roleBytes);

    Mockito.when(connector.compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
        arg,
        roleName,
        updatedAuthorizerPermissions
    ))))
           .thenReturn(true);

    Map<String, KeycloakAuthorizerRole> expectedRoleMap = ImmutableMap.of(
        roleName, new KeycloakAuthorizerRole(roleName, updatedAuthorizerPermissions));
    byte[] expectedRoleMapBytes = objectMapper.writeValueAsBytes(expectedRoleMap);
    KeycloakAuthorizerRoleMapBundle expectedRoleMapBundle = new KeycloakAuthorizerRoleMapBundle(
        expectedRoleMap,
        expectedRoleMapBytes
    );

    storageUpdater.start();
    storageUpdater.setPermissions(roleName, updatedPermissions);
    Assert.assertEquals(expectedRoleMap, storageUpdater.getCachedRoleMap());

    Mockito.verify(connector)
           .compareAndSwap(ArgumentMatchers.argThat((arg) -> roleUpdatedWithPermissions(
               arg,
               roleName,
               updatedAuthorizerPermissions
           )));
    Mockito.verify(cacheNotifier).scheduleUpdate();
  }

  @Test(expected = KeycloakSecurityDBResourceException.class)
  public void test_setPermissions_roleDoesNotExist_fails()
  {
    String roleName = "test_setPermissions_roleDoesNotExist_fails";
    ResourceAction resourceAction = new ResourceAction(
        new Resource("datasourc1", ResourceType.DATASOURCE),
        Action.READ
    );
    List<ResourceAction> updatedPermissions = ImmutableList.of(
        resourceAction
    );
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(ImmutableMap.of());
    Mockito.when(connector.lookup(
        "config",
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        "keycloak_authorization_roles"
    )).thenReturn(null);

    storageUpdater.start();
    storageUpdater.setPermissions(roleName, updatedPermissions);

    Mockito.verify(connector, Mockito.never()).compareAndSwap(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).setUpdateSource(ArgumentMatchers.any());
    Mockito.verify(cacheNotifier, Mockito.never()).scheduleUpdate();
  }

  private boolean roleUpdatedWithPermissions(
      List<MetadataCASUpdate> updates,
      String roleName,
      List<KeycloakAuthorizerPermission> permissions
  )
  {
    try {
      if (updates == null) {
        return false;
      }
      Map<String, KeycloakAuthorizerRole> roleMap = objectMapper.readValue(
          (updates.get(0)).getNewValue(),
          KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
      );
      KeycloakAuthorizerRole role = roleMap.get(roleName);
      return role != null && role.getPermissions().equals(permissions);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean roleDeleted(
      List<MetadataCASUpdate> updates,
      String roleName
  )
  {
    try {
      if (updates == null) {
        return false;
      }
      Map<String, KeycloakAuthorizerRole> oldRoleMap = objectMapper.readValue(
          (updates.get(0)).getOldValue(),
          KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
      );
      Map<String, KeycloakAuthorizerRole> updatedRoleMap = objectMapper.readValue(
          (updates.get(0)).getNewValue(),
          KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
      );
      return oldRoleMap != null
             && updatedRoleMap != null
             && oldRoleMap.get(roleName) != null
             && updatedRoleMap.get(roleName) == null;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
