/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.util.Objects;

public class KeycloakAuthCommonCacheConfig
{
  private static final long DEFAULT_POLLING_PERIOD = 60000;
  private static final long DEFAULT_MAX_RANDOM_DELAY = DEFAULT_POLLING_PERIOD / 10;
  private static final int DEFAULT_MAX_SYNC_RETRIES = 10;
  private static final long DEFAULT_CACHE_NOTIFY_TIMEOUT_MS = 5000;
  private static final long DEFAULT_NOTIFIER_UPDATE_PERIOD = DEFAULT_POLLING_PERIOD / 10;

  @JsonProperty
  private final long pollingPeriod;

  @JsonProperty
  private final long maxRandomDelay;

  @JsonProperty
  @Nullable
  private final String cacheDirectory;

  @JsonProperty
  private final int maxSyncRetries;

  @JsonProperty
  private final boolean enableCacheNotifications;

  @JsonProperty
  private final long cacheNotificationTimeout;

  @JsonProperty
  private final long notifierUpdatePeriod;

  @JsonProperty
  private final boolean enforceNotBeforePolicies;

  @JsonProperty
  private final boolean autoPopulateAdmin;

  @JsonProperty
  @Nullable
  private final String initialRoleMappingFile;

  @JsonCreator
  public KeycloakAuthCommonCacheConfig(
      @JsonProperty("pollingPeriod") @Nullable Long pollingPeriod,
      @JsonProperty("maxRandomDelay") @Nullable Long maxRandomDelay,
      @JsonProperty("cacheDirectory") @Nullable String cacheDirectory,
      @JsonProperty("maxSyncRetries") @Nullable Integer maxSyncRetries,
      @JsonProperty("enableCacheNotifications") @Nullable Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") @Nullable Long cacheNotificationTimeout,
      @JsonProperty("notifierUpdatePeriod") @Nullable Long notifierUpdatePeriod,
      @JsonProperty("enforceNotBeforePolicies") @Nullable Boolean enforceNotBeforePolicies,
      @JsonProperty("autoPopulateAdmin") @Nullable Boolean autoPopulateAdmin,
      @JsonProperty("initialRoleMappingFile") @Nullable String initialRoleMappingFile
  )
  {
    this.pollingPeriod = pollingPeriod == null ? DEFAULT_POLLING_PERIOD : pollingPeriod;
    this.maxRandomDelay = maxRandomDelay == null ? DEFAULT_MAX_RANDOM_DELAY : maxRandomDelay;
    this.cacheDirectory = cacheDirectory;
    this.maxSyncRetries = maxSyncRetries == null ? DEFAULT_MAX_SYNC_RETRIES : maxSyncRetries;
    this.enableCacheNotifications = enableCacheNotifications == null || enableCacheNotifications;
    this.cacheNotificationTimeout = cacheNotificationTimeout == null ? DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout;
    this.notifierUpdatePeriod = notifierUpdatePeriod == null ? DEFAULT_NOTIFIER_UPDATE_PERIOD : notifierUpdatePeriod;
    this.enforceNotBeforePolicies = enforceNotBeforePolicies == null || enforceNotBeforePolicies;
    this.autoPopulateAdmin = autoPopulateAdmin == null || autoPopulateAdmin;
    this.initialRoleMappingFile = initialRoleMappingFile;
  }

  @VisibleForTesting
  public KeycloakAuthCommonCacheConfig()
  {
    this.pollingPeriod = DEFAULT_POLLING_PERIOD;
    this.maxRandomDelay = DEFAULT_MAX_RANDOM_DELAY;
    this.cacheDirectory = null;
    this.maxSyncRetries = DEFAULT_MAX_SYNC_RETRIES;
    this.enableCacheNotifications = true;
    this.cacheNotificationTimeout = DEFAULT_CACHE_NOTIFY_TIMEOUT_MS;
    this.notifierUpdatePeriod = DEFAULT_NOTIFIER_UPDATE_PERIOD;
    this.enforceNotBeforePolicies = true;
    this.autoPopulateAdmin = true;
    this.initialRoleMappingFile = null;
  }

  @JsonProperty
  public long getPollingPeriod()
  {
    return pollingPeriod;
  }

  @JsonProperty
  public long getMaxRandomDelay()
  {
    return maxRandomDelay;
  }

  @JsonProperty
  @Nullable
  public String getCacheDirectory()
  {
    return cacheDirectory;
  }

  @JsonProperty
  public int getMaxSyncRetries()
  {
    return maxSyncRetries;
  }

  @JsonProperty
  public long getCacheNotificationTimeout()
  {
    return cacheNotificationTimeout;
  }

  @JsonProperty
  public boolean isEnableCacheNotifications()
  {
    return enableCacheNotifications;
  }


  @JsonProperty
  public long getNotifierUpdatePeriod()
  {
    return notifierUpdatePeriod;
  }

  @JsonProperty
  public boolean isEnforceNotBeforePolicies()
  {
    return enforceNotBeforePolicies;
  }

  @JsonProperty
  public boolean isAutoPopulateAdmin()
  {
    return autoPopulateAdmin;
  }

  @JsonProperty
  @Nullable
  public String getInitialRoleMappingFile()
  {
    return initialRoleMappingFile;
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
    KeycloakAuthCommonCacheConfig that = (KeycloakAuthCommonCacheConfig) o;
    return pollingPeriod == that.pollingPeriod
           && maxRandomDelay == that.maxRandomDelay
           && maxSyncRetries == that.maxSyncRetries
           && enableCacheNotifications == that.enableCacheNotifications
           && cacheNotificationTimeout == that.cacheNotificationTimeout
           && notifierUpdatePeriod == that.notifierUpdatePeriod
           && enforceNotBeforePolicies == that.enforceNotBeforePolicies
           && autoPopulateAdmin == that.autoPopulateAdmin
           && Objects.equals(initialRoleMappingFile, that.initialRoleMappingFile)
           && Objects.equals(cacheDirectory, that.cacheDirectory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        pollingPeriod,
        maxRandomDelay,
        cacheDirectory,
        maxSyncRetries,
        enableCacheNotifications,
        cacheNotificationTimeout,
        notifierUpdatePeriod,
        enforceNotBeforePolicies,
        autoPopulateAdmin,
        initialRoleMappingFile
    );
  }
}
