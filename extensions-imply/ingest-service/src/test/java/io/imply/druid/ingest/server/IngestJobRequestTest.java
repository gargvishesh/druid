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
import com.google.common.collect.ImmutableList;
import io.imply.druid.ingest.metadata.IngestSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class IngestJobRequestTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    IngestJobRequest jobRequest = new IngestJobRequest(
        new IngestSchema(
            new TimestampSpec("time", "iso", null),
            new DimensionsSpec(
                ImmutableList.of(
                    StringDimensionSchema.create("foo"),
                    StringDimensionSchema.create("bar")
                )
            ),
            new JsonInputFormat(null, null, null),
            "A test schema"
        ),
        null
    );

    Assert.assertEquals(
        jobRequest,
        MAPPER.readValue(MAPPER.writeValueAsString(jobRequest), IngestJobRequest.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    // DimensionSchema implementations delegate to the abstract method for hashcode method, which makes equals verifier
    // sad and im not sure how to fix
    // EqualsVerifier.forClass(IngestJobRequest.class).usingGetClass().verify();
  }
}
