/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ProvisionInstancesResponseTest
{
  private static final DefaultObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String createdNodeId = "new-node-id";
    ProvisionInstancesResponse response = new ProvisionInstancesResponse(
        ImmutableList.of(
            new ProvisionInstancesResponse.ProvisionInstanceResponse("versionx", ImmutableList.of(createdNodeId))
        )
    );
    String json = OBJECT_MAPPER.writeValueAsString(response);
    ProvisionInstancesResponse response2 = OBJECT_MAPPER.readValue(json, ProvisionInstancesResponse.class);
    Assert.assertEquals(response, response2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ProvisionInstancesResponse.class)
                  .withNonnullFields("instances")
                  .usingGetClass()
                  .verify();
  }
}
