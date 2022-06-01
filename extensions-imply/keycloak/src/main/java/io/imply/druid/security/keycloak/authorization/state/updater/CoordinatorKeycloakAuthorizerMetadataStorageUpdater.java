/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.updater;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.KeycloakSecurityDBResourceException;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleMapBundle;
import io.imply.druid.security.keycloak.authorization.state.notifier.KeycloakAuthorizerCacheNotifier;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.joda.time.Duration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorKeycloakAuthorizerMetadataStorageUpdater implements KeycloakAuthorizerMetadataStorageUpdater
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorKeycloakAuthorizerMetadataStorageUpdater.class);

  private static final TypeReference<Map<String, List<ResourceAction>>> AUTO_POPULATE_FILE_TYPE_REFERENCE =
      new TypeReference<Map<String, List<ResourceAction>>>()
      {
      };

  private static final long UPDATE_RETRY_DELAY = 1000;

  private static final String ROLES = "roles";

  public static final List<ResourceAction> SUPERUSER_PERMISSIONS = AuthorizationUtils.makeSuperUserPermissions();

  private final Injector injector;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final KeycloakAuthorizerCacheNotifier cacheNotifier;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsonMapper;
  private volatile KeycloakAuthorizerRoleMapBundle roleMapBundle;
  private final KeycloakAuthCommonCacheConfig commonCacheConfig;

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final int numRetries = 5;

  private final ScheduledExecutorService exec;
  private volatile boolean stopped = false;
  private volatile byte[] roleMapSerialized;

  @Inject
  public CoordinatorKeycloakAuthorizerMetadataStorageUpdater(
      Injector injector,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      @Smile ObjectMapper smileMapper,
      @Json ObjectMapper jsonMapper,
      KeycloakAuthorizerCacheNotifier cacheNotifier,
      KeycloakAuthCommonCacheConfig commonCacheConfig
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorKeycloakAuthorizerMetadataStorageUpdater-Exec--%d");
    this.injector = injector;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.smileMapper = smileMapper;
    this.jsonMapper = jsonMapper;
    this.cacheNotifier = cacheNotifier;
    this.commonCacheConfig = commonCacheConfig;
    this.roleMapBundle = null;
    if (commonCacheConfig.isEnableCacheNotifications()) {
      this.cacheNotifier.setRoleUpdateSource(
          () -> roleMapSerialized
      );
    }
  }

  @VisibleForTesting
  public CoordinatorKeycloakAuthorizerMetadataStorageUpdater()
  {
    this.exec = null;
    this.injector = null;
    this.connector = null;
    this.connectorConfig = null;
    this.smileMapper = null;
    this.jsonMapper = null;
    this.cacheNotifier = null;
    this.commonCacheConfig = null;
    this.roleMapBundle = null;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    AuthorizerMapper authorizerMapper = injector.getInstance(AuthorizerMapper.class);
    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return;
    }

    try {
      // config table must exist, else this is going to be sad
      connector.createConfigTable();
      LOG.info("Starting CoordinatorKeycloakAuthorizerMetadataStorageUpdater");
      KeycloakAuthUtils.maybeInitialize(
          () -> {
            for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
              Authorizer authorizer = entry.getValue();
              if (authorizer instanceof ImplyKeycloakAuthorizer) {
                byte[] roleMapBytes = getCurrentRoleMapBytes();
                Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
                    smileMapper,
                    roleMapBytes
                );
                synchronized (this) {
                  roleMapBundle = new KeycloakAuthorizerRoleMapBundle(roleMap, roleMapBytes);
                }
                initAdminRoleAndPermissions(roleMap);
              }
            }
            return true;
          });

      Duration initalDelay = new Duration(commonCacheConfig.getPollingPeriod());
      Duration delay = new Duration(commonCacheConfig.getPollingPeriod());
      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          initalDelay,
          delay,
          () -> {
            if (stopped) {
              return ScheduledExecutors.Signal.STOP;
            }
            try {
              LOG.debug("Scheduled db poll is running");
              byte[] roleMapBytes = getCurrentRoleMapBytes();
              Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(
                  smileMapper,
                  roleMapBytes
              );
              if (roleMapBytes != null) {
                synchronized (this) {
                  roleMapBundle = new KeycloakAuthorizerRoleMapBundle(roleMap, roleMapBytes);
                }
              }
              LOG.debug("Scheduled db poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedRoleMap.").emit();
            }
            return ScheduledExecutors.Signal.REPEAT;
          }
      );
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

    LOG.info("CoordinatorKeycloakAuthorizerMetadataStorageUpdater is stopping.");
    stopped = true;
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
  @Nullable
  public Map<String, KeycloakAuthorizerRole> getCachedRoleMap()
  {
    return roleMapBundle == null ? null : roleMapBundle.getRoleMap();
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

  @Override
  public void refreshAllNotification()
  {
    synchronized (this) {
      serializeCache();
    }
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
    Map<String, KeycloakAuthorizerRole> roleMap = KeycloakAuthUtils.deserializeAuthorizerRoleMap(smileMapper, oldValue);
    if (roleMap.get(roleName) != null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] already exists.", roleName);
    } else {
      roleMap.put(roleName, new KeycloakAuthorizerRole(roleName, null));
    }
    byte[] newValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(smileMapper, roleMap);
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
        smileMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
    } else {
      roleMap.remove(roleName);
    }

    byte[] newRoleMapValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(smileMapper, roleMap);

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
          synchronized (this) {
            roleMapBundle = new KeycloakAuthorizerRoleMapBundle(roleMap, newRoleMapValue);
            serializeCache();
          }

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
        smileMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new KeycloakSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }
    roleMap.put(
        roleName,
        new KeycloakAuthorizerRole(roleName, KeycloakAuthorizerPermission.makePermissionList(permissions))
    );
    byte[] newRoleMapValue = KeycloakAuthUtils.serializeAuthorizerRoleMap(smileMapper, roleMap);

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

  private void initAdminRoleAndPermissions(Map<String, KeycloakAuthorizerRole> roleMap)
  {
    if (commonCacheConfig.isAutoPopulateAdmin()) {
      if (commonCacheConfig.getInitialRoleMappingFile() != null) {
        initializeFromFile(roleMap);
      } else if (!roleMap.containsKey(KeycloakAuthUtils.ADMIN_NAME)) {
        // built-in
        createRoleInternal(KeycloakAuthUtils.ADMIN_NAME);
        setPermissionsInternal(KeycloakAuthUtils.ADMIN_NAME, SUPERUSER_PERMISSIONS);
      }
    }
  }

  private void initializeFromFile(Map<String, KeycloakAuthorizerRole> roleMap)
  {
    // since this is only called by initAdminRoleAndPermissions we can use assert here instead of Preconditions
    assert commonCacheConfig.getInitialRoleMappingFile() != null;
    File mappingFile = new File(commonCacheConfig.getInitialRoleMappingFile());
    if (!mappingFile.exists()) {
      LOG.warn("Skipping auto-populate, role to permission map file [%s] does not exist", mappingFile.getAbsolutePath());
      return;
    }
    try {
      Map<String, List<ResourceAction>> fromDisk = jsonMapper.readValue(
          mappingFile,
          AUTO_POPULATE_FILE_TYPE_REFERENCE
      );
      for (Map.Entry<String, List<ResourceAction>> roleEntry : fromDisk.entrySet()) {
        if (!roleMap.containsKey(roleEntry.getKey())) {
          createRoleInternal(roleEntry.getKey());
        }
        setPermissionsInternal(roleEntry.getKey(), roleEntry.getValue());
      }
    }
    catch (IOException e) {
      LOG.error(
          e,
          "Failed to auto-populate role to permission mappings from file [%s]",
          mappingFile.getAbsolutePath()
      );
    }
  }

  /**
   * Should only be called within a synchronized (this) block
   */
  @GuardedBy("this")
  private void serializeCache()
  {
    try {
      roleMapSerialized = smileMapper.writeValueAsBytes(getCachedRoleMap());
      cacheNotifier.scheduleRoleUpdate();
    }
    catch (JsonProcessingException e) {
      throw new ISE(e, "Failed to JSON-Smile serialize cached view definitions");
    }
  }

  private static String getPrefixedKeyColumn(String keyName)
  {
    return StringUtils.format("keycloak_authorization_%s", keyName);
  }
}
