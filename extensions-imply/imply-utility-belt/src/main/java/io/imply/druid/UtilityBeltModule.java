/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.imply.druid.cloudwatch.CloudWatchInputRowParser;
import io.imply.druid.currency.CurrencySumAggregatorFactory;
import io.imply.druid.fastrack.GeoIpExprMacro;
import io.imply.druid.fastrack.GeoIpSqlOperator;
import io.imply.druid.fastrack.UserAgentExprMacro;
import io.imply.druid.fastrack.UserAgentSqlOperator;
import io.imply.druid.spatial.GeohashExprMacro;
import io.imply.druid.spatial.GeohashSqlOperatorConversion;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class UtilityBeltModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
            new NamedType(CloudWatchInputRowParser.class, CloudWatchInputRowParser.TYPE_NAME),
            new NamedType(CurrencySumAggregatorFactory.class, CurrencySumAggregatorFactory.TYPE_NAME)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "imply.utility.belt", UtilityBeltConfig.class);
    LifecycleModule.register(binder, GeoIpExprMacro.class);
    ExpressionModule.addExprMacro(binder, UserAgentExprMacro.class);
    ExpressionModule.addExprMacro(binder, GeoIpExprMacro.class);
    ExpressionModule.addExprMacro(binder, GeohashExprMacro.class);
    SqlBindings.addOperatorConversion(binder, GeoIpSqlOperator.class);
    SqlBindings.addOperatorConversion(binder, UserAgentSqlOperator.class);
    SqlBindings.addOperatorConversion(binder, GeohashSqlOperatorConversion.class);
  }
}
