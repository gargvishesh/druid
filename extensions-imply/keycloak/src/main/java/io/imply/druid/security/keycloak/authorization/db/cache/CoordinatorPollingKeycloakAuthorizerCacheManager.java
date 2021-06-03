/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.ImplyKeycloakAuthorizer;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import io.imply.druid.security.keycloak.KeycloakAuthUtils;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorPollingKeycloakAuthorizerCacheManager implements KeycloakAuthorizerCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingKeycloakAuthorizerCacheManager.class);

  private final Injector injector;
  private final KeycloakAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper;
  private final DruidLeaderClient druidLeaderClient;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final ScheduledExecutorService exec;

  private Map<String, KeycloakAuthorizerRole> cachedRoleMap;

  @Inject
  public CoordinatorPollingKeycloakAuthorizerCacheManager(
      Injector injector,
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorPollingKeycloakAuthorizerCacheManager-Exec--%d");
    this.injector = injector;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cachedRoleMap = new ConcurrentHashMap<>();
    this.druidLeaderClient = druidLeaderClient;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    LOG.info("Starting CoordinatorPollingKeycloakAuthorizerCacheManager.");

    try {
      boolean isKeycloakAuthorizerConfigured = isKeycloakAuthorizerConfigured();
      if (!isKeycloakAuthorizerConfigured) {
        LOG.warn("Did not find any keycloak authorizer confgured. Not polling coordinator for roleMap updates");
        return;
      }
      initRoleMaps();

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled roleMap cache poll is running");
              Map<String, KeycloakAuthorizerRole> roleMap = fetchRoleMapFromCoordinator(false);
              if (roleMap != null) {
                cachedRoleMap = roleMap;
              }

              LOG.debug("Scheduled roleMap cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedRoleMaps.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started CoordinatorPollingKeycloakAuthorizerCacheManager.");
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

    LOG.info("CoordinatorPollingKeycloakAuthorizerCacheManager is stopping.");
    exec.shutdown();
    LOG.info("CoordinatorPollingKeycloakAuthorizerCacheManager is stopped.");
  }

  @Override
  public void handleAuthorizerRoleUpdate(byte[] serializedRoleMap)
  {
    LOG.debug("Received roleMap cache update for keycloak authorizer.");
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {

      cachedRoleMap = objectMapper.readValue(
          serializedRoleMap,
          KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
      );

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeRoleMapToDisk(serializedRoleMap);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize role map received from coordinator").emit();
    }
  }

  @Override
  public Map<String, KeycloakAuthorizerRole> getRoleMap()
  {
    return cachedRoleMap;
  }

  private void writeRoleMapToDisk(byte[] roleMapBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    File roleMapFile = new File(commonCacheConfig.getCacheDirectory(), getRoleMapFilename());
    LOG.debug("Writing role map to file [%s]", roleMapFile.getAbsolutePath());
    writeFileAtomically(roleMapFile, roleMapBytes);
  }

  @VisibleForTesting
  void writeFileAtomically(File roleMapFile, byte[] roleMapBytes) throws IOException
  {
    FileUtils.writeAtomically(
        roleMapFile,
        out -> {
          out.write(roleMapBytes);
          return null;
        }
    );
  }

  private void initRoleMaps()
  {
    Map<String, KeycloakAuthorizerRole> roleMap = fetchRoleMapFromCoordinator(true);
    if (roleMap != null) {
      cachedRoleMap = roleMap;
    }
  }

  private boolean isKeycloakAuthorizerConfigured()
  {
    AuthorizerMapper authorizerMapper = injector.getInstance(AuthorizerMapper.class);

    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return false;
    }

    for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
      Authorizer authorizer = entry.getValue();
      if (authorizer instanceof ImplyKeycloakAuthorizer) {
        return true;
      }
    }

    return false;
  }

  @Nullable
  private Map<String, KeycloakAuthorizerRole> fetchRoleMapFromCoordinator(boolean isInit)
  {
    try {
      return RetryUtils.retry(
          this::tryFetchRoleMapFromCoordinator,
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching role map for keycloak authorizer").emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load role map snapshot from disk.");
            return loadRoleMapFromDisk();
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading role map snapshot for keycloak authorizer")
               .emit();
          }
        }
      }
      return null;
    }
  }

  private Map<String, KeycloakAuthorizerRole> tryFetchRoleMapFromCoordinator() throws Exception
  {
    Map<String, KeycloakAuthorizerRole> roleMap = null;
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/keycloak-security/authorization/cachedSerializedRoleMap")
    );
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] roleMapBytes = responseHolder.getContent();

    if (roleMapBytes != null) {
      roleMap = objectMapper.readValue(
          roleMapBytes,
          KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
      );
      if (roleMap != null && commonCacheConfig.getCacheDirectory() != null) {
        writeRoleMapToDisk(roleMapBytes);
      }
    } else {
      LOG.info("null cached serialized role map retrieved, keycloak authenticator");
    }

    return roleMap;
  }

  @Nullable
  @VisibleForTesting
  Map<String, KeycloakAuthorizerRole> loadRoleMapFromDisk() throws IOException
  {
    File roleMapFile = new File(commonCacheConfig.getCacheDirectory(), getRoleMapFilename());
    if (!roleMapFile.exists()) {
      LOG.debug("role map file [%s] does not exist", roleMapFile.getAbsolutePath());
      return null;
    }
    return objectMapper.readValue(
        roleMapFile,
        KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE
    );
  }

  private String getRoleMapFilename()
  {
    return "keycloakAuthorizer.role.cache";
  }
}
