package io.imply.druid.security.keycloak.authorization.db.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorKeycloakAuthorizerMetadataStorageUpdater implements KeycloakAuthorizerMetadataStorageUpdater
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorKeycloakAuthorizerMetadataStorageUpdater.class);

  private static final long UPDATE_RETRY_DELAY = 1000;

  private static final String ROLES = "roles";

  private final AuthorizerMapper authorizerMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final ObjectMapper objectMapper;

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final int numRetries = 5;

  public static final List<ResourceAction> SUPERUSER_PERMISSIONS = makeSuperUserPermissions();

  @Inject
  public CoordinatorKeycloakAuthorizerMetadataStorageUpdater(
      AuthorizerMapper authorizerMapper,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      @Smile ObjectMapper objectMapper
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.objectMapper = objectMapper;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return;
    }

    try {
      LOG.info("Starting CoordinatorKeycloakAuthorizerMetadataStorageUpdater");
      KeycloakAuthUtils.maybeInitialize(
          () -> {
            for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
              Authorizer authorizer = entry.getValue();
              if (authorizer instanceof ImplyKeycloakAuthorizer) {
                byte[] roleMapBytes = getCurrentRoleMapBytes();
                Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
                    objectMapper,
                    roleMapBytes
                );
                initAdminRoleAndPermissions(roleMap);
              }
            }
            return true;
          });

      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    LOG.info("CoordinatorKeycloakAuthorizerMetadataStorageUpdater is stopped.");
  }

  @Override
  public byte[] getCurrentRoleMapBytes()
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(ROLES)
    );
  }

  @Override
  public void createRole(String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    createRoleInternal(roleName);
  }

  @Override
  public void deleteRole(String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    deleteRoleInternal(roleName);
  }

  @Override
  public void setPermissions(String roleName, List<ResourceAction> permissions)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    setPermissionsInternal(roleName, permissions);
  }

  private void createRoleInternal(String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (createRoleOnce(roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not create role [%s] due to concurrent update contention.", roleName);
  }

  private boolean createRoleOnce(String roleName)
  {
    byte[] oldValue = getCurrentRoleMapBytes();
    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(objectMapper, oldValue);
    if (roleMap.get(roleName) != null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] already exists.", roleName);
    } else {
      roleMap.put(roleName, new KeycloakAuthorizerRole(roleName, null));
    }
    byte[] newValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);
    return tryUpdateRoleMap(roleMap, oldValue, newValue);
  }

  private void deleteRoleInternal(String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (deleteRoleOnce(roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not delete role [%s] due to concurrent update contention.", roleName);
  }

  private boolean deleteRoleOnce(String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes();
    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
    } else {
      roleMap.remove(roleName);
    }

    byte[] newRoleMapValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);

    return tryUpdateRoleMap(
        roleMap,
        oldRoleMapValue,
        newRoleMapValue
    );
  }

  private boolean tryUpdateRoleMap(
      Map<String, KeycloakAuthorizerRole> roleMap,
      byte[] oldRoleMapValue,
      byte[] newRoleMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (roleMap != null) {
        updates.add(
            createMetadataCASUpdate(oldRoleMapValue, newRoleMapValue, ROLES)
        );

        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {
          // TODO: update caches when they are implemeted.
          return true;
        } else {
          return false;
        }
      }
      return false;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setPermissionsInternal(String roleName, List<ResourceAction> permissions)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (setPermissionsOnce(roleName, permissions)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not set permissions for role [%s] due to concurrent update contention.", roleName);
  }

  private boolean setPermissionsOnce(String roleName, List<ResourceAction> permissions)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes();
    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }
    roleMap.put(
        roleName,
        new KeycloakAuthorizerRole(roleName, KeycloakAuthorizerPermission.makePermissionList(permissions))
    );
    byte[] newRoleMapValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);

    return tryUpdateRoleMap(roleMap, oldRoleMapValue, newRoleMapValue);
  }

  @Nonnull
  private MetadataCASUpdate createMetadataCASUpdate(
      byte[] oldValue,
      byte[] newValue,
      String columnName
  )
  {
    return new MetadataCASUpdate(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(columnName),
        oldValue,
        newValue
    );
  }

  private void initAdminRoleAndPermissions(
      Map<String, KeycloakAuthorizerRole> roleMap
  )
  {
    if (!roleMap.containsKey(KeycloakAuthUtils.ADMIN_NAME)) {
      createRoleInternal(KeycloakAuthUtils.ADMIN_NAME);
      setPermissionsInternal(KeycloakAuthUtils.ADMIN_NAME, SUPERUSER_PERMISSIONS);
    }
  }

  private static String getPrefixedKeyColumn(String keyName)
  {
    return StringUtils.format("keycloak_authorization_%s", keyName);
  }

  private static List<ResourceAction> makeSuperUserPermissions()
  {
    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    return Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);
  }
}