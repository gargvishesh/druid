/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ImplyViewDefinitionTest
{
  private final ObjectMapper objectMapper;

  public ImplyViewDefinitionTest()
  {
    this.objectMapper = new DefaultObjectMapper();
  }

  @Test
  public void test_serde() throws Exception
  {
    ImplyViewDefinition definition = new ImplyViewDefinition(
        "cloned_foo",
        "SELECT * FROM foo"
    );

    final ImplyViewDefinition fromJson = objectMapper.readValue(
        objectMapper.writeValueAsBytes(definition),
        ImplyViewDefinition.class
    );

    Assert.assertEquals(definition, fromJson);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ImplyViewDefinition.class).usingGetClass().withNonnullFields("viewName", "viewSql").verify();
  }
}
