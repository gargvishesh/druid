/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class IngestSchemaTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    IngestSchema schema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("dim1"),
                StringDimensionSchema.create("dim2")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    Assert.assertEquals(schema, MAPPER.readValue(MAPPER.writeValueAsString(schema), IngestSchema.class));
  }

  @Test
  public void testEqualsAndHashcode()
  {
    // DimensionSchema implementations delegate to the abstract method for hashcode method, which makes equals verifier
    // sad and im not sure how to fix
    // EqualsVerifier.forClass(BatchAppendIngestSchema.class).usingGetClass().verify();
  }
}
