/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.license.ImplyLicenseManager;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SampledAvgScoreObjectSqlAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SampledAvgScoreToEstimateOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SampledAvgScoreToHistogramOperatorConversion;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class ImplyArrayOfDoublesSketchModule implements DruidModule
{
  static final String AD_TECH_INVENTORY = "adTechInventory";
  static final String SAMPLED_AVG_SCORE_FEATURE_NAME = "sampled-avg-score";
  static final String AD_TECH_AGGREGATORS_FEATURE_NAME = "ad-tech-aggregators";
  static final String SAMPLED_AVG_SCORE = "sampledAvgScore";
  static final String SAMPLED_AVG_SCORE_HISTOGRAM = "sampledAvgScoreHistogram";
  private ImplyLicenseManager implyLicenseManager;
  private final Logger log = new Logger(ImplyArrayOfDoublesSketchModule.class);

  @Inject
  public void setImplyLicenseManager(ImplyLicenseManager implyLicenseManager)
  {
    this.implyLicenseManager = implyLicenseManager;
  }

  @Override
  public void configure(Binder binder)
  {
    if (implyLicenseManager.isFeatureEnabled(SAMPLED_AVG_SCORE_FEATURE_NAME)) {
      configureSampledAvgScore(binder);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    ImmutableList.Builder<SimpleModule> jacksonModules = new ImmutableList.Builder<>();

    if (implyLicenseManager.isFeatureEnabled(SAMPLED_AVG_SCORE_FEATURE_NAME)) {
      log.info("The %s feature is enabled", SAMPLED_AVG_SCORE_FEATURE_NAME);
      jacksonModules.add(sampledAvgScoreJacksonModule());
    }
    if (implyLicenseManager.isFeatureEnabled(AD_TECH_AGGREGATORS_FEATURE_NAME)) {
      log.info("The %s feature is enabled", AD_TECH_AGGREGATORS_FEATURE_NAME);
      jacksonModules.add(adTechAggregatorsJacksonModule());
    }
    return jacksonModules.build();
  }

  private void configureSampledAvgScore(Binder binder)
  {
    SqlBindings.addAggregator(binder, SampledAvgScoreObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, SampledAvgScoreToHistogramOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SampledAvgScoreToEstimateOperatorConversion.class);
  }

  private SimpleModule sampledAvgScoreJacksonModule()
  {
    return new SimpleModule(SAMPLED_AVG_SCORE_FEATURE_NAME).registerSubtypes(
        new NamedType(SampledAvgScoreAggregatorFactory.class, SAMPLED_AVG_SCORE),
        new NamedType(SampledAvgScoreToHistogramPostAggregator.class, SAMPLED_AVG_SCORE_HISTOGRAM));
  }

  private SimpleModule adTechAggregatorsJacksonModule()
  {
    return new SimpleModule(AD_TECH_AGGREGATORS_FEATURE_NAME).registerSubtypes(
        new NamedType(AdTechInventoryAggregatorFactory.class, AD_TECH_INVENTORY)
    );
  }
}
