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

public class ImplyAutoScaleModuleTest
{
  @Test
  public void testConfig()
  {
    final String json = "{\n"
                        + "   \"envConfig\" : {\n"
                        + "      \"implyAddress\" : \"implymanager.io\",\n"
                        + "      \"clusterId\" : \"cluster-id-123\"\n"
                        + "   },\n"
                        + "   \"maxNumWorkers\" : 5,\n"
                        + "   \"minNumWorkers\" : 2,\n"
                        + "   \"type\" : \"imply\"\n"
                        + "}";

    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new ImplyAutoScaleModule().getJacksonModules());
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
      final ImplyAutoScaler autoScaler = (ImplyAutoScaler) objectMapper.readValue(json, AutoScaler.class);
      Assert.assertEquals("cluster-id-123", autoScaler.getEnvConfig().getClusterId());
      Assert.assertEquals("implymanager.io", autoScaler.getEnvConfig().getImplyAddress());
      Assert.assertEquals(5, autoScaler.getMaxNumWorkers());
      Assert.assertEquals(2, autoScaler.getMinNumWorkers());

      final ImplyAutoScaler roundTripAutoScaler = (ImplyAutoScaler) objectMapper.readValue(
          objectMapper.writeValueAsBytes(autoScaler),
          AutoScaler.class
      );
      Assert.assertEquals("cluster-id-123", roundTripAutoScaler.getEnvConfig().getClusterId());
      Assert.assertEquals("implymanager.io", roundTripAutoScaler.getEnvConfig().getImplyAddress());
      Assert.assertEquals(5, roundTripAutoScaler.getMaxNumWorkers());
      Assert.assertEquals(2, roundTripAutoScaler.getMinNumWorkers());

      Assert.assertEquals("Round trip equals", autoScaler, roundTripAutoScaler);
    }
    catch (Exception e) {
      Assert.fail(StringUtils.format("Got exception in test %s", e.getMessage()));
    }
  }
}
