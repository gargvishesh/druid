/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;

public class ImplyTokenSerdeTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDimensionsSpecSerde() throws Exception
  {
    ImplyToken expected = new ImplyToken(
        "user",
        1234L,
        Lists.newArrayList(
            new ResourceAction(
                new Resource("ds1", ResourceType.DATASOURCE),
                Action.READ
            ),
            new ResourceAction(
                new Resource("ds2", ResourceType.DATASOURCE),
                Action.WRITE
            )
        ),
        null
    );

    String jsonStr = "{\"user\":\"user\",\"expiry\":1234,\"permissions\":[{\"resource\":{\"name\":\"ds1\",\"type\":\"DATASOURCE\"},\"action\":\"READ\"},{\"resource\":{\"name\":\"ds2\",\"type\":\"DATASOURCE\"},\"action\":\"WRITE\"}], \"appUser\":{\"hello\":\"world\", \"foo\":\"bar\"}}";

    ImplyToken actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, ImplyToken.class)
        ),
        ImplyToken.class
    );

    Assert.assertEquals(expected, actual);
  }
}
