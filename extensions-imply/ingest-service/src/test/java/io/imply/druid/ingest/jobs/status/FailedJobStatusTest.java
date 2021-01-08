/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class FailedJobStatusTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    FailedJobStatus failedStatus = new FailedJobStatus("oh noes");

    String serialized = MAPPER.writeValueAsString(failedStatus);
    FailedJobStatus andBack = MAPPER.readValue(serialized, FailedJobStatus.class);
    Assert.assertEquals(failedStatus, andBack);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(FailedJobStatus.class).usingGetClass().verify();
  }
}
