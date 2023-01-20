/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

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
