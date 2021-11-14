/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.license.ImplyLicenseManager;
import io.imply.druid.timeseries.aggregation.DeltaTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.MeanTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;

public class TimeSeriesModule implements DruidModule
{
  static final String TIMESERIES_FEATURE_NAME = "timeseries";
  private static final String SIMPLE_TIMESERIES = "timeseries";
  private static final String AVG_TIMESERIES = "avgTimeseries";
  private static final String DELTA_TIMESERIES = "deltaTimeseries";
  private ImplyLicenseManager implyLicenseManager;
  private final Logger log = new Logger(TimeSeriesModule.class);

  @Inject
  public void setImplyLicenseManager(ImplyLicenseManager implyLicenseManager)
  {
    this.implyLicenseManager = implyLicenseManager;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (!implyLicenseManager.isFeatureEnabled(TIMESERIES_FEATURE_NAME)) {
      return ImmutableList.of();
    }

    log.info("The imply-timeseries feature is enabled");
    return ImmutableList.of(
        new SimpleModule("TimeSeriesModule").registerSubtypes(
            new NamedType(
                SimpleTimeSeriesAggregatorFactory.class,
                SIMPLE_TIMESERIES
            ),
            new NamedType(
                MeanTimeSeriesAggregatorFactory.class,
                AVG_TIMESERIES
            ),
            new NamedType(
                DeltaTimeSeriesAggregatorFactory.class,
                DELTA_TIMESERIES
            )
        ));
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
