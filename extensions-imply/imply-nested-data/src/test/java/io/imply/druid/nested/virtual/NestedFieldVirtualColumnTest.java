/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.virtual;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class NestedFieldVirtualColumnTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    NestedFieldVirtualColumn virtualColumn = new NestedFieldVirtualColumn("nested", ".x.y.z", "v0", ColumnType.LONG);
    Assert.assertEquals(virtualColumn, JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(virtualColumn), NestedFieldVirtualColumn.class));
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedFieldVirtualColumn.class).withNonnullFields("columnName", "path", "outputName").withIgnoredFields("parts").usingGetClass().verify();
  }
}
