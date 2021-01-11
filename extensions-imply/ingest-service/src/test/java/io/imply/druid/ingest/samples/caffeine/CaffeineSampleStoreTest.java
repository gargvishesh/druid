/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.caffeine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CaffeineSampleStoreTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  CaffeineSampleStore sampleStore;

  @Before
  public void setup()
  {
    sampleStore = new CaffeineSampleStore(new CaffeineSampleStoreConfig(), MAPPER);
  }

  @Test
  public void testStoreAndRetrieve() throws IOException
  {
    final String jobId = UUIDUtils.generateUuid();
    final SamplerResponse samplerResponse = new SamplerResponse(
        2,
        2,
        ImmutableList.of(
            new SamplerResponse.SamplerResponseRow(
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                false,
                null
            )
        )
    );
    sampleStore.storeSample(jobId, samplerResponse);

    SamplerResponse roundTrip = sampleStore.getSamplerResponse(jobId);

    Assert.assertEquals(samplerResponse, roundTrip);

    sampleStore.deleteSample(jobId);

    Assert.assertNull(sampleStore.getSamplerResponse(jobId));
  }
}
