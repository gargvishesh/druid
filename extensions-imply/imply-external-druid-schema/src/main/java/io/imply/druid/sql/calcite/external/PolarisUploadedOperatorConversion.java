/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.google.inject.Inject;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.sql.calcite.external.DruidUserDefinedTableMacroConversion;

public class PolarisUploadedOperatorConversion extends DruidUserDefinedTableMacroConversion
{
  @Inject
  public PolarisUploadedOperatorConversion(
      final PolarisTableFunctionResolver resolver,
      final TableDefnRegistry registry
  )
  {
    super(
        PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec.FUNCTION_NAME,
        new PolarisUploadedInputSourceDefn(registry, resolver).adHocTableFn(),
        registry.jsonMapper()
    );
  }
}
