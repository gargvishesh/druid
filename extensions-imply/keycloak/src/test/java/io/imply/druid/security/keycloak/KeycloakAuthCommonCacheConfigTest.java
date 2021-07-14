/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KeycloakAuthCommonCacheConfigTest
{
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private KeycloakAuthCommonCacheConfig target;

  @Test
  public void test_serde_defaults() throws IOException
  {
    target = new KeycloakAuthCommonCacheConfig(null, null, null, null, null, null, null, null, null, null);

    Assert.assertEquals(60000L, target.getPollingPeriod());
    Assert.assertEquals(6000L, target.getMaxRandomDelay());
    Assert.assertNull(target.getCacheDirectory());
    Assert.assertEquals(10, target.getMaxSyncRetries());
    Assert.assertTrue(target.isEnableCacheNotifications());
    Assert.assertEquals(5000L, target.getCacheNotificationTimeout());
    Assert.assertEquals(6000L, target.getNotifierUpdatePeriod());
    Assert.assertTrue(target.isEnforceNotBeforePolicies());
    Assert.assertTrue(target.isAutoPopulateAdmin());
    Assert.assertNull(target.getInitialRoleMappingFile());

    byte[] cacheConfigBytes = objectMapper.writeValueAsBytes(target);
    KeycloakAuthCommonCacheConfig cacheConfigDeserialized = objectMapper.readValue(
        cacheConfigBytes,
        KeycloakAuthCommonCacheConfig.class
    );

    Assert.assertEquals(target.getPollingPeriod(), cacheConfigDeserialized.getPollingPeriod());
    Assert.assertEquals(target.getMaxRandomDelay(), cacheConfigDeserialized.getMaxRandomDelay());
    Assert.assertNull(cacheConfigDeserialized.getCacheDirectory());
    Assert.assertEquals(target.getMaxSyncRetries(), cacheConfigDeserialized.getMaxSyncRetries());
  }

  @Test
  public void test_serde_nonDefaults() throws IOException
  {
    target = new KeycloakAuthCommonCacheConfig(100L, 200L, "/tmp/cache", 5, false, 1000L, 2000L, false, false, "foo.json");

    Assert.assertEquals(100L, target.getPollingPeriod());
    Assert.assertEquals(200L, target.getMaxRandomDelay());
    Assert.assertEquals("/tmp/cache", target.getCacheDirectory());
    Assert.assertEquals(5, target.getMaxSyncRetries());
    Assert.assertFalse(target.isEnableCacheNotifications());
    Assert.assertEquals(1000L, target.getCacheNotificationTimeout());
    Assert.assertEquals(2000L, target.getNotifierUpdatePeriod());

    byte[] cacheConfigBytes = objectMapper.writeValueAsBytes(target);
    KeycloakAuthCommonCacheConfig cacheConfigDeserialized = objectMapper.readValue(
        cacheConfigBytes,
        KeycloakAuthCommonCacheConfig.class
    );

    Assert.assertEquals(target, cacheConfigDeserialized);
  }
}
