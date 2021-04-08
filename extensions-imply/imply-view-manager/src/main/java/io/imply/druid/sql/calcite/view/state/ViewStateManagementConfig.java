/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ViewStateManagementConfig
{
  private static final long DEFAULT_POLLING_PERIOD = 60000;
  private static final long DEFAULT_NOTIFIER_UPDATE_PERIOD = DEFAULT_POLLING_PERIOD / 10;
  private static final long DEFAULT_MAX_RANDOM_DELAY = DEFAULT_POLLING_PERIOD / 10;
  private static final int DEFAULT_MAX_SYNC_RETRIES = 5;
  public static final long DEFAULT_CACHE_NOTIFY_TIMEOUT_MS = 5000;

  @JsonProperty
  private final long pollingPeriod;

  @JsonProperty
  private final long maxRandomDelay;

  @JsonProperty
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
  private final boolean allowUnrestrictedViews;

  @JsonCreator
  public ViewStateManagementConfig(
      @JsonProperty("pollingPeriod") Long pollingPeriod,
      @JsonProperty("maxRandomDelay") Long maxRandomDelay,
      @JsonProperty("cacheDirectory") String cacheDirectory,
      @JsonProperty("maxSyncRetries") Integer maxSyncRetries,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("notifierUpdatePeriod") Long notifierUpdatePeriod,
      @JsonProperty("allowUnrestrictedViews") Boolean allowUnrestrictedViews
  )
  {
    this.pollingPeriod = pollingPeriod == null ? DEFAULT_POLLING_PERIOD : pollingPeriod;
    this.maxRandomDelay = maxRandomDelay == null ? DEFAULT_MAX_RANDOM_DELAY : maxRandomDelay;
    this.cacheDirectory = cacheDirectory;
    this.maxSyncRetries = maxSyncRetries == null ? DEFAULT_MAX_SYNC_RETRIES : maxSyncRetries;
    this.enableCacheNotifications = enableCacheNotifications == null ? true : enableCacheNotifications;
    this.cacheNotificationTimeout = cacheNotificationTimeout == null
                                    ? DEFAULT_CACHE_NOTIFY_TIMEOUT_MS
                                    : cacheNotificationTimeout;
    this.notifierUpdatePeriod = notifierUpdatePeriod == null ? DEFAULT_NOTIFIER_UPDATE_PERIOD : notifierUpdatePeriod;
    this.allowUnrestrictedViews = allowUnrestrictedViews == null ? false : allowUnrestrictedViews;
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
  public boolean isEnableCacheNotifications()
  {
    return enableCacheNotifications;
  }

  @JsonProperty
  public long getCacheNotificationTimeout()
  {
    return cacheNotificationTimeout;
  }

  @JsonProperty
  public boolean isAllowUnrestrictedViews()
  {
    return allowUnrestrictedViews;
  }

  @JsonProperty
  public long getNotifierUpdatePeriod()
  {
    return notifierUpdatePeriod;
  }
}
