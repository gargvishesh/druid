package io.imply.druid.autoscaling.server;

import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ListInstancesResponseTest
{
  private static final DefaultObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String instanceStatus1 = "RUNNING";
    String instanceIp1 = "1.1.1.1";
    String instanceId1 = "id1";
    String instanceStatus2 = "TERMINATING";
    String instanceIp2 = "2.2.2.2";
    String instanceId2 = "id2";
    ListInstancesResponse response = new ListInstancesResponse(
        ImmutableList.of(
            new ListInstancesResponse.Instance(instanceStatus1, instanceIp1, instanceId1),
            new ListInstancesResponse.Instance(instanceStatus2, instanceIp2, instanceId2)
        )
    );
    String json = OBJECT_MAPPER.writeValueAsString(response);
    ListInstancesResponse response2 = OBJECT_MAPPER.readValue(json, ListInstancesResponse.class);
    Assert.assertEquals(response, response2);
  }
}
