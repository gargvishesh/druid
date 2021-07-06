package io.imply.druid.autoscaling.server;

import com.google.common.collect.ImmutableList;
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
    ProvisionInstancesResponse response = new ProvisionInstancesResponse(ImmutableList.of(createdNodeId));

    String json = OBJECT_MAPPER.writeValueAsString(response);
    ProvisionInstancesResponse response2 = OBJECT_MAPPER.readValue(json, ProvisionInstancesResponse.class);
    Assert.assertEquals(response, response2);
  }
}
