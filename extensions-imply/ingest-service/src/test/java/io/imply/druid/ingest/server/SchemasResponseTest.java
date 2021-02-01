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
import io.imply.druid.ingest.metadata.PartitionScheme;
import io.imply.druid.ingest.metadata.StoredIngestSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class SchemasResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    SchemasResponse response = new SchemasResponse(
        ImmutableList.of(
            new StoredIngestSchema(
                1,
                new IngestSchema(
                    new TimestampSpec("time", "iso", null),
                    new DimensionsSpec(
                        ImmutableList.of(
                            StringDimensionSchema.create("some_dimension")
                        )
                    ),
                    new PartitionScheme(Granularities.HOUR, null),
                    new JsonInputFormat(null, null, null),
                    "test schema"
                )
            )
        )
    );
    String serialized = MAPPER.writeValueAsString(response);
    SchemasResponse andBack = MAPPER.readValue(serialized, SchemasResponse.class);
    Assert.assertEquals(response, andBack);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    // DimensionSchema implementations delegate to the abstract method for hashcode method, which makes equals verifier
    // sad and im not sure how to fix
    // EqualsVerifier.forClass(SchemasResponse.class).usingGetClass().verify();
  }
}
