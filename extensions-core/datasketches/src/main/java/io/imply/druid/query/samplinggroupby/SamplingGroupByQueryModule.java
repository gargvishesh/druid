/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.imply.druid.query.samplinggroupby.metrics.DefaultSamplingGroupByQueryMetricsFactory;
import io.imply.druid.query.samplinggroupby.metrics.SamplingGroupByQueryMetricsFactory;
import io.imply.druid.query.samplinggroupby.sql.SamplingRateSqlAggregator;
import io.imply.druid.query.samplinggroupby.sql.calcite.rule.DruidSamplingGroupByQueryRule;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

@LoadScope(roles = {NodeRole.ROUTER_JSON_NAME, NodeRole.BROKER_JSON_NAME, NodeRole.HISTORICAL_JSON_NAME, NodeRole.PEON_JSON_NAME})
public class SamplingGroupByQueryModule implements DruidModule
{
  public static final String SAMPLING_GROUPBY_QUERY_METRICS_FACTORY_PROPERTY = "druid.query.samplingGroupBy.queryMetricsFactory";

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);
    toolChests.addBinding(SamplingGroupByQuery.class).to(SamplingGroupByQueryToolChest.class);
    binder.bind(SamplingGroupByQueryToolChest.class).in(LazySingleton.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder = DruidBinders.queryRunnerFactoryBinder(
        binder
    );

    queryFactoryBinder.addBinding(SamplingGroupByQuery.class).to(SamplingGroupByQueryRunnerFactory.class);
    binder.bind(SamplingGroupByQueryRunnerFactory.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        SAMPLING_GROUPBY_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(SamplingGroupByQueryMetricsFactory.class),
        Key.get(DefaultSamplingGroupByQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(SamplingGroupByQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultSamplingGroupByQueryMetricsFactory.class);

    SqlBindings.addAggregator(binder, SamplingRateSqlAggregator.class);

    // bind the SamplingGroupBy planning rule
    Multibinder.newSetBinder(binder, ExtensionCalciteRuleProvider.class)
               .addBinding()
               .to(DruidSamplingGroupByQueryRule.DruidSamplingGroupByQueryRuleProvider.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("SamplingGroupBy").registerSubtypes(
            new NamedType(SamplingGroupByQuery.class, SamplingGroupByQuery.QUERY_TYPE),
            new NamedType(SamplingRateAggregatorFactory.class, SamplingRateSqlAggregator.NAME)
        )
    );
  }
}