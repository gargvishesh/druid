package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.sql.calcite.external.CatalogExternalTableOperatorConversion;

public class PolarisUploadedOperatorConversion extends CatalogExternalTableOperatorConversion
{
  public static final String FUNCTION_NAME = "POLARIS_UPLOADED";

  @Inject
  public PolarisUploadedOperatorConversion(
      @Json final ObjectMapper jsonMapper,
      final PolarisTableFunctionResolver resolver)
  {
    super(FUNCTION_NAME, new PolarisUploadedInputSourceDefn(resolver).adHocTableFn(), jsonMapper);
  }
}
