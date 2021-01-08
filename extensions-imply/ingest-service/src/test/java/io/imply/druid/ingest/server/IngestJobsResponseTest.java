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
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.PartitionScheme;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class IngestJobsResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    IngestJobsResponse response = new IngestJobsResponse(
        ImmutableList.of(
            new IngestJobInfo(
                "some_table",
                UUIDUtils.generateUuid(),
                JobState.SCHEDULED,
                DateTimes.nowUtc().minus(60 * 1000),
                DateTimes.nowUtc(),
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
                ),
                null
            )
        )
    );
    String serialized = MAPPER.writeValueAsString(response);
    IngestJobsResponse andBack = MAPPER.readValue(serialized, IngestJobsResponse.class);
    Assert.assertEquals(response, andBack);
  }
}
