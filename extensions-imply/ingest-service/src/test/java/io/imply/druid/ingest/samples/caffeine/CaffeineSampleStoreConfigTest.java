/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.caffeine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class CaffeineSampleStoreConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final String json = "{\"expireTimeMinutes\": 120, \"maxSizeBytes\":123456}";
    CaffeineSampleStoreConfig config = MAPPER.readValue(json, CaffeineSampleStoreConfig.class);
    Assert.assertEquals(120L, config.getCacheExpireTimeMinutes());
    Assert.assertEquals(123456L, config.getMaxSizeBytes());
    CaffeineSampleStoreConfig config2 = MAPPER.readValue(json, CaffeineSampleStoreConfig.class);
    Assert.assertEquals(config, config2);
    Assert.assertEquals(config.hashCode(), config2.hashCode());
  }
}
