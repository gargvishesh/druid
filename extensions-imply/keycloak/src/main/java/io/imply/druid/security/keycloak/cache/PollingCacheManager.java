/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.cache;

import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.Duration;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Lifecycle managed
 */
public abstract class PollingCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(PollingCacheManager.class);

  private final ScheduledExecutorService exec;
  protected final LifecycleLock lifecycleLock = new LifecycleLock();
  protected final CopyOnWriteArrayList<PollingManagedCache<?>> caches;
  protected final KeycloakAuthCommonCacheConfig commonCacheConfig;

  public PollingCacheManager(
      KeycloakAuthCommonCacheConfig commonCacheConfig
  )
  {
    this.exec = Execs.scheduledSingleThreaded("PollingCacheManager-Exec--%d");
    this.commonCacheConfig = commonCacheConfig;
    this.caches = new CopyOnWriteArrayList<>();
  }

  /**
   * the name of this cache manager, for logging purposes
   */
  public abstract String getCacheManagerName();

  /**
   * should {@link #start()} do things? For example, configuration can be validated, allowing the manager to enter a
   * peaceful slumber
   */
  public abstract boolean shouldStart();

  /**
   * Add a cache
   */
  protected void addCache(PollingManagedCache<?> cache)
  {
    this.caches.add(cache);
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    LOG.info("Starting PollingCacheManager for %s.", getCacheManagerName());

    try {
      boolean shouldStart = shouldStart();
      if (!shouldStart) {
        LOG.warn("Cache indicated it should not start. Not polling coordinator for cache updates");
        return;
      }

      for (PollingManagedCache<?> cache : caches) {
        try {
          cache.initializeCacheValue();
        }
        catch (Throwable t) {
          LOG.error(t, "Failed to initialized cache %s", cache.getCacheName());
        }
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled %s cache poll is running", getCacheManagerName());
              for (PollingManagedCache<?> cache : caches) {
                try {
                  cache.refreshCacheValue();
                }
                catch (Throwable t) {
                  LOG.error(t, "Failed to refresh cache %s", cache.getCacheName());
                }
              }

              LOG.debug("Scheduled %s cache poll is done", getCacheManagerName());
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling to refresh cache for %s.", getCacheManagerName()).emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started PollingCacheManager for %s.", getCacheManagerName());
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

    LOG.info("PollingCacheManager for %s is stopping.", getCacheManagerName());
    exec.shutdown();
    LOG.info("PollingCacheManager for %s is stopped.", getCacheManagerName());
  }
}
