/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import io.imply.druid.sql.calcite.schema.tables.mapping.ExternalTableFunctionApiMapperImpl;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class PolarisTableFunctionResolverTest
{
  private static PolarisTableFunctionResolverImpl resolver;

  @Before
  public void setup()
  {
    ExternalTableFunctionApiMapperImpl tblFnMapper = Mockito.spy(
        new ExternalTableFunctionApiMapperImpl(
            Mockito.mock(ImplyExternalDruidSchemaCommonConfig.class),
            new ObjectMapper(),
            Mockito.mock(HttpClient.class)
        ));

    resolver = new PolarisTableFunctionResolverImpl(tblFnMapper);
    Mockito.doReturn(PolarisTableFunctionTestUtil.TEST_EXTERNAL_TABLE_SPEC)
           .when(tblFnMapper).getTableFunctionMapping(ArgumentMatchers.any());
  }

  @Test
  public void test_validTableFunctionSpec_resolves_expectedExtTableSpec()
  {
    PolarisExternalTableSpec resolvedExternalTableSpec = resolver.resolve(PolarisTableFunctionTestUtil.TEST_TABLE_FUNCTION_SPEC);
    Assert.assertEquals(PolarisTableFunctionTestUtil.TEST_EXTERNAL_TABLE_SPEC, resolvedExternalTableSpec);
  }

  @Test
  public void test_nullTableFunctionSpec_resolve_ThrowsException()
  {
    IAE exception = Assert.assertThrows(IAE.class, () -> resolver.resolve(null));
    Assert.assertTrue(exception.getMessage().contains("Polaris table function spec must be provided."));
  }
}
