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
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class PolarisUploadedFunctionSpecTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec spec =
        new PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec(ImmutableList.of("file.json"));

    PolarisTableFunctionSpec specSerde = MAPPER.readValue(
        MAPPER.writeValueAsString(spec),
        PolarisTableFunctionSpec.class);

    Assert.assertEquals(spec, specSerde);
  }
}
