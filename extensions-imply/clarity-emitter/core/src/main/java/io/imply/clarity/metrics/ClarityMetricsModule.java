/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.metrics;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByQueryMetricsFactory;
import org.apache.druid.query.timeseries.TimeseriesQueryMetricsFactory;
import org.apache.druid.query.topn.TopNQueryMetricsFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ClarityMetricsModule implements DruidModule
{
  public static final String CLARITY_METRICS_NAME = "clarity";
  private static final String PROPERTY_COEXIST = "druid.emitter.clarity.metricsFactoryChoice";
  @Inject
  private Properties props;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    if (Boolean.valueOf(props.getProperty(PROPERTY_COEXIST, "false"))) {
      // Add choices through PolyBind.
      PolyBind
          .optionBinder(binder, Key.get(GenericQueryMetricsFactory.class))
          .addBinding(CLARITY_METRICS_NAME)
          .to(ClarityGenericQueryMetricsFactory.class);

      PolyBind
          .optionBinder(binder, Key.get(GroupByQueryMetricsFactory.class))
          .addBinding(CLARITY_METRICS_NAME)
          .to(ClarityGroupByQueryMetricsFactory.class);

      PolyBind
          .optionBinder(binder, Key.get(TopNQueryMetricsFactory.class))
          .addBinding(CLARITY_METRICS_NAME)
          .to(ClarityTopNQueryMetricsFactory.class);

      PolyBind
          .optionBinder(binder, Key.get(TimeseriesQueryMetricsFactory.class))
          .addBinding(CLARITY_METRICS_NAME)
          .to(ClarityTimeseriesQueryMetricsFactory.class);
    } else {
      // Impose our will!
      binder.bind(GenericQueryMetricsFactory.class).to(ClarityGenericQueryMetricsFactory.class);
      binder.bind(GroupByQueryMetricsFactory.class).to(ClarityGroupByQueryMetricsFactory.class);
      binder.bind(TopNQueryMetricsFactory.class).to(ClarityTopNQueryMetricsFactory.class);
      binder.bind(TimeseriesQueryMetricsFactory.class).to(ClarityTimeseriesQueryMetricsFactory.class);
    }
  }
}
