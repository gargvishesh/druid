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
import com.google.inject.Inject;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.sql.calcite.external.CatalogExternalTableOperatorConversion;

public class PolarisUploadedOperatorConversion extends CatalogExternalTableOperatorConversion
{
  @Inject
  public PolarisUploadedOperatorConversion(
      @Json final ObjectMapper jsonMapper,
      final PolarisTableFunctionResolver resolver,
      final TableDefnRegistry registry)
  {
    super(
        PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec.FUNCTION_NAME,
        new PolarisUploadedInputSourceDefn(registry, resolver).adHocTableFn(),
        jsonMapper
    );
  }
}
