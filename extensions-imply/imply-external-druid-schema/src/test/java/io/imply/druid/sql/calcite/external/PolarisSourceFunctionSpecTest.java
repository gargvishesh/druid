package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class PolarisSourceFunctionSpecTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    PolarisSourceOperatorConversion.PolarisSourceFunctionSpec spec =
        new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec("< polaris source >");

    PolarisTableFunctionSpec specSerde = MAPPER.readValue(
        MAPPER.writeValueAsString(spec),
        PolarisTableFunctionSpec.class);

    Assert.assertEquals(spec, specSerde);
  }
}
