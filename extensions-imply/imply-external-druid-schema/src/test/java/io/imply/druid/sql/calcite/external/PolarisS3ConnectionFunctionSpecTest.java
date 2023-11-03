/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import java.util.EnumSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.junit.Assert;
import org.junit.Test;

public class PolarisS3ConnectionFunctionSpecTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException {
    PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec spec =
        new PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec(
            "sampleConn",
            ImmutableList.of("foo"),
            null,
            ImmutableList.of("obj1"),
            "foo/*.json",
            new SystemFields(EnumSet.of(SystemField.BUCKET, SystemField.PATH, SystemField.URI))
        );

    PolarisTableFunctionSpec specSerde = MAPPER.readValue(
        MAPPER.writeValueAsString(spec),
        PolarisTableFunctionSpec.class);

    Assert.assertEquals(spec, specSerde);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.simple().forClass(PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec.class).verify();
  }
}

