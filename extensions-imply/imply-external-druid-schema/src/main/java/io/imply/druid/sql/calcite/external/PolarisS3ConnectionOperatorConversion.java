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

public class PolarisS3ConnectionOperatorConversion extends DruidUserDefinedTableMacroConversion
{
  @Inject
  public PolarisS3ConnectionOperatorConversion(
      final PolarisTableFunctionResolver resolver,
      final TableDefnRegistry registry
  )
  {
    super(
        PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec.FUNCTION_NAME,
        new PolarisS3ConnectionInputSourceDefn(registry, resolver).adHocTableFn(),
        registry.jsonMapper()
    );
  }
}
