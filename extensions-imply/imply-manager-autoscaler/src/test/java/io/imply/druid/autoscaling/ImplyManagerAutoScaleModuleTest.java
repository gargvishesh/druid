/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class ImplyManagerAutoScaleModuleTest
{
  @Test
  public void testConfig()
  {
    final String json = "{\n"
                        + "   \"envConfig\" : {\n"
                        + "      \"implyManagerAddress\" : \"implymanager.io\",\n"
                        + "      \"clusterId\" : \"cluster-id-123\"\n"
                        + "   },\n"
                        + "   \"maxNumWorkers\" : 5,\n"
                        + "   \"minNumWorkers\" : 2,\n"
                        + "   \"type\" : \"implyManager\"\n"
                        + "}";

    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new ImplyManagerAutoScaleModule().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object o,
              DeserializationContext deserializationContext,
              BeanProperty beanProperty,
              Object o1
          )
          {
            return null;
          }
        }
    );

    try {
      final ImplyManagerAutoScaler autoScaler = (ImplyManagerAutoScaler) objectMapper.readValue(json, AutoScaler.class);
      Assert.assertEquals("cluster-id-123", autoScaler.getEnvConfig().getClusterId());
      Assert.assertEquals("implymanager.io", autoScaler.getEnvConfig().getImplyManagerAddress());
      Assert.assertEquals(5, autoScaler.getMaxNumWorkers());
      Assert.assertEquals(2, autoScaler.getMinNumWorkers());

      final ImplyManagerAutoScaler roundTripAutoScaler = (ImplyManagerAutoScaler) objectMapper.readValue(
          objectMapper.writeValueAsBytes(autoScaler),
          AutoScaler.class
      );
      Assert.assertEquals("cluster-id-123", roundTripAutoScaler.getEnvConfig().getClusterId());
      Assert.assertEquals("implymanager.io", roundTripAutoScaler.getEnvConfig().getImplyManagerAddress());
      Assert.assertEquals(5, roundTripAutoScaler.getMaxNumWorkers());
      Assert.assertEquals(2, roundTripAutoScaler.getMinNumWorkers());

      Assert.assertEquals("Round trip equals", autoScaler, roundTripAutoScaler);
    }
    catch (Exception e) {
      Assert.fail(StringUtils.format("Got exception in test %s", e.getMessage()));
    }
  }
}
