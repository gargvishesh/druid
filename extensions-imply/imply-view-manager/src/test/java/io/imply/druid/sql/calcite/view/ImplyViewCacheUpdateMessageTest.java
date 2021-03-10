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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ImplyViewCacheUpdateMessageTest
{
  private final ObjectMapper objectMapper;

  public ImplyViewCacheUpdateMessageTest()
  {
    this.objectMapper = new DefaultObjectMapper();
  }

  @Test
  public void test_serde() throws Exception
  {
    ImplyViewCacheUpdateMessage message = new ImplyViewCacheUpdateMessage(
        ImmutableMap.of(
            "cloned_foo",
            new ImplyViewDefinition(
                "cloned_foo",
                "SELECT * FROM foo"
            ),
            "filter_bar",
            new ImplyViewDefinition(
                "filter_bar",
                "SELECT * FROM bar WHERE col = 'a'"
            )
        )
    );

    final ImplyViewCacheUpdateMessage fromJson = objectMapper.readValue(
        objectMapper.writeValueAsBytes(message),
        ImplyViewCacheUpdateMessage.class
    );

    Assert.assertEquals(message, fromJson);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ImplyViewCacheUpdateMessage.class).usingGetClass().withNonnullFields("views").verify();
  }
}
