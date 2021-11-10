/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.imply.druid.security.keycloak.KeycloakAuthCommonCacheConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * Basic cache container, with facilities to periodically refresh data from a source ({@link #fetchData}),
 * store and retrieve this data from disk ({@link #readCachedDataFromDisk}, {@link #writeCachedDataToDisk}), and to
 * handle out of band 'push' notifications of  complete updates ({@link #handleUpdate}).
 *
 * Best used with {@link PollingCacheManager}, which is a lifecycle bound state manager class which can be loaded up
 * with a collection of {@link PollingManagedCache} to maintain with fresh data over the lifetime of a Druid service.
 */
public abstract class PollingManagedCache<T>
{
  private static final EmittingLogger LOG = new EmittingLogger(PollingManagedCache.class);

  protected final String cacheName;
  protected final KeycloakAuthCommonCacheConfig commonCacheConfig;
  protected final ObjectMapper objectMapper;

  public PollingManagedCache(
      String cacheName,
      KeycloakAuthCommonCacheConfig commonCacheConfig,
      ObjectMapper objectMapper
  )
  {
    this.cacheName = cacheName;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
  }

  /**
   * Fetch data and populate the cache, probably making use of {@link #fetchData} and {@link #readCachedDataFromDisk()}
   */
  protected abstract void initializeCacheValue();
  /**
   * Fetch data and populate the cache, probably making use of {@link #fetchData}
   */
  protected abstract void refreshCacheValue();

  /**
   * Update the cache value. This might be called concurrently if {@link #refreshCacheValue()} is running while
   * an external push notification also attempts to update the cache value
   */
  protected abstract T handleUpdate(byte[] serializedData);

  @Nullable
  protected abstract T getCacheValue();

  /**
   * Get the exact type reference of the cache value, used for serializing/deserializing the cache to/from disk during
   * transient failures
   */
  protected abstract TypeReference<T> getTypeReference();

  /**
   * Try to fetch the latest cache data
   */
  @Nullable
  protected abstract byte[] tryFetchDataForPath(String path) throws Exception;

  /**
   * Wraps retries around calls to {@link #tryFetchDataForPath}, attempting to fall back to a disk cached value if a
   * failure happens during initialization
   */
  @Nullable
  protected T fetchData(String path, boolean isInit)
  {
    try {
      final byte[] cacheValueBytes = RetryUtils.retry(
          () -> tryFetchDataForPath(path),
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
      final T cacheValue;
      if (cacheValueBytes != null) {
        cacheValue = handleUpdate(cacheValueBytes);
      } else {
        cacheValue = null;
        LOG.info("null serialized cache map retrieved for [%s]", cacheName);
      }
      return cacheValue;
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching fresh data for [%s]", cacheName).emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load [%s] cache snapshot from disk.", cacheName);
            return readCachedDataFromDisk();
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading cached data snapshot for [%s]", cacheName)
               .emit();
          }
        }
      }
      return null;
    }
  }

  @VisibleForTesting
  @Nullable
  public T readCachedDataFromDisk() throws IOException
  {
    File cacheFile = new File(commonCacheConfig.getCacheDirectory(), getCacheFilename());
    if (!cacheFile.exists()) {
      LOG.debug("cache map file [%s] does not exist", cacheFile.getAbsolutePath());
      return null;
    }
    return objectMapper.readValue(
        cacheFile,
        getTypeReference()
    );
  }

  protected void writeCachedDataToDisk(byte[] cacheBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    FileUtils.mkdirp(cacheDir);
    File cacheFile = new File(commonCacheConfig.getCacheDirectory(), getCacheFilename());
    LOG.debug("Writing cached data to file [%s]", cacheFile.getAbsolutePath());
    writeFileAtomically(cacheFile, cacheBytes);
  }

  @VisibleForTesting
  public void writeFileAtomically(File cacheFile, byte[] cachedDataBytes) throws IOException
  {
    FileUtils.writeAtomically(
        cacheFile,
        out -> {
          out.write(cachedDataBytes);
          return null;
        }
    );
  }


  public String getCacheName()
  {
    return cacheName;
  }

  protected String getCacheFilename()
  {
    return StringUtils.format("%s.cache", cacheName);
  }
}
