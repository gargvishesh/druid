/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class IngestServiceSqlMetatadataConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerdeDefaults() throws JsonProcessingException
  {
    IngestServiceSqlMetatadataConfig defaultConfig = IngestServiceSqlMetatadataConfig.DEFAULT_CONFIG;
    Assert.assertEquals(
        defaultConfig,
        MAPPER.readValue(MAPPER.writeValueAsString(defaultConfig), IngestServiceSqlMetatadataConfig.class)
    );
  }


  @Test
  public void testSerde() throws JsonProcessingException
  {
    IngestServiceSqlMetatadataConfig defaultConfig = new IngestServiceSqlMetatadataConfig(
        false,
        "some_custom_jobs_table",
        "some_custom_tables_table"
    );
    Assert.assertEquals(
        defaultConfig,
        MAPPER.readValue(MAPPER.writeValueAsString(defaultConfig), IngestServiceSqlMetatadataConfig.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(IngestServiceSqlMetatadataConfig.class).usingGetClass().verify();
  }
}
