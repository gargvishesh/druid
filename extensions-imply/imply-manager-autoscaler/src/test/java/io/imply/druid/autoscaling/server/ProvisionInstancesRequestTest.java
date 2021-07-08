/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ProvisionInstancesRequestTest
{
  private static final DefaultObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String workerVersion = "123";
    int numToCreate = 2;
    ProvisionInstancesRequest request = new ProvisionInstancesRequest(workerVersion, numToCreate);

    String json = OBJECT_MAPPER.writeValueAsString(request);
    ProvisionInstancesRequest request2 = OBJECT_MAPPER.readValue(json, ProvisionInstancesRequest.class);
    Assert.assertEquals(request, request2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ProvisionInstancesRequest.class)
                  .withNonnullFields("numToCreate", "version")
                  .usingGetClass()
                  .verify();
  }
}
