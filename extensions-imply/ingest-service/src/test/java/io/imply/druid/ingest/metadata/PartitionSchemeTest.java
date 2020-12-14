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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Assert;
import org.junit.Test;

public class PartitionSchemeTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PartitionScheme.class).usingGetClass().verify();
  }

  @Test
  public void testJsonSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final PartitionScheme partitionScheme = new PartitionScheme(
        Granularities.MONTH,
        new HashedPartitionsSpec(100, null, ImmutableList.of("dim"))
    );
    final String json = mapper.writeValueAsString(partitionScheme);
    final PartitionScheme fromJson = mapper.readValue(json, PartitionScheme.class);
    Assert.assertEquals(partitionScheme, fromJson);
  }
}
