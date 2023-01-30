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
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonCacheConfig;
import io.imply.druid.sql.calcite.schema.tables.mapping.ExternalTableFunctionApiMapperImpl;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;

public class PolarisSourceFunctionResolverTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static PolarisSourceFunctionResolver resolver;

  @Before
  public void setup() throws Exception
  {
    ExternalTableFunctionApiMapperImpl tblFnMapper = Mockito.spy(
        new ExternalTableFunctionApiMapperImpl(
            Mockito.mock(ImplyExternalDruidSchemaCommonCacheConfig.class),
            Mockito.mock(HttpClient.class)
        ));

    resolver = new PolarisSourceFunctionResolver(tblFnMapper, new ObjectMapper());
    Mockito.doReturn(MAPPER.writeValueAsBytes(getSamplePolarisExternalTableSpec()))
           .when(tblFnMapper).getTableFunctionMapping(ArgumentMatchers.any());
  }

  @Test
  public void test_validTableFunctionSpec_resolves_expectedExtTableSpec()
  {
    PolarisExternalTableSpec resolvedExternalTableSpec = resolver.resolve(getSampleTableFunctionSpec());
    Assert.assertEquals(getSamplePolarisExternalTableSpec(), resolvedExternalTableSpec);
  }

  @Test
  public void test_nullTableFunctionSpec_resolve_ThrowsException()
  {
    IAE exception = Assert.assertThrows(IAE.class, () -> resolver.resolve(null));
    Assert.assertTrue(exception.getMessage().contains("Polaris table function spec must be provided."));
  }

  private PolarisExternalTableSpec getSamplePolarisExternalTableSpec()
  {
    LocalInputSource localInputSource = new LocalInputSource(
        null,
        null,
        Collections.singletonList(
            new File("/tmp/84c90e1d-3430-4814-bfa6-d256487419c6/b90145b3-1ce8-4246-8d6a-b7f58f28076e"))
    );
    JsonInputFormat jsonInputFormat = new JsonInputFormat(null, null, null, true, null);
    return new PolarisExternalTableSpec(
        localInputSource,
        jsonInputFormat,
        null
    );
  }

  private PolarisTableFunctionSpec getSampleTableFunctionSpec()
  {
    return new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec(
        "{\"fileList\":[\"data.json\"],\"formatSettings\":{\"format\":\"nd-json\"},\"type\":\"uploaded\"}"
    );
  }
}
