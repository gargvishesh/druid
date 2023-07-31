/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.sql.guice.SqlBindings;
import java.util.List;

public class SketchUtilModule implements DruidModule
{

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    ExpressionModule.addExprMacro(binder, GcpHllToApacheHllExpression.class);
    SqlBindings.addOperatorConversion(binder, GcpHllToApacheHllExpression.HLLPPToHLLConversion.class);
  }
}
