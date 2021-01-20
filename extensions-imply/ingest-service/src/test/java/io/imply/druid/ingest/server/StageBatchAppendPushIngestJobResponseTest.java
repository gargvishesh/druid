/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class StageBatchAppendPushIngestJobResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws URISyntaxException, JsonProcessingException
  {
    StageBatchAppendPushIngestJobResponse response = new StageBatchAppendPushIngestJobResponse(
        BatchAppendJobRunner.generateTaskId(UUIDUtils.generateUuid()),
        new URI("s3://some/s3/object")
    );

    Assert.assertEquals(response, MAPPER.readValue(MAPPER.writeValueAsString(response), StageBatchAppendPushIngestJobResponse.class));
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(StageBatchAppendPushIngestJobResponse.class).usingGetClass().verify();
  }
}
