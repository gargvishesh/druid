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
import io.imply.druid.query.aggregation.datasketches.expressions.MurmurHashExprMacros;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.AdTechInventorySqlAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.MurmurHashOperatorConversions;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreObjectSqlAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToEstimateOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToHistogramFilteringOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToHistogramOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionFilterOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.virtual.ImplySessionFilteringVirtualColumn;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class ImplyArrayOfDoublesSketchModule implements DruidModule
{
  static final String AD_TECH_INVENTORY = "adTechInventory";
  static final String SAMPLED_AVG_SCORE_FEATURE_NAME = "sampled-avg-score";
  static final String AD_TECH_AGGREGATORS_FEATURE_NAME = "ad-tech-aggregators";
  static final String SESSIONIZATION_FEATURE_NAME = "sessionization";
  static final String SESSION_AVG_SCORE = "sessionAvgScore";
  static final String SESSION_AVG_SCORE_HISTOGRAM = "sessionAvgScoreHistogram";
  static final String SESSION_AVG_SCORE_HISTOGRAM_FILTERING = "sessionAvgScoreHistogramFiltering";
  static final String SESSION_FILTERING_VIRTUAL_COLUMN = "session-filtering";
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
    if (implyLicenseManager.isFeatureEnabled(SAMPLED_AVG_SCORE_FEATURE_NAME) ||
        implyLicenseManager.isFeatureEnabled(SESSIONIZATION_FEATURE_NAME)) {
      configureSessionAvgScore(binder);
      ExpressionModule.addExprMacro(binder, MurmurHashExprMacros.Murmur3Macro.class);
      ExpressionModule.addExprMacro(binder, MurmurHashExprMacros.Murmur3_64Macro.class);
    }

    if (implyLicenseManager.isFeatureEnabled(AD_TECH_AGGREGATORS_FEATURE_NAME) ||
        implyLicenseManager.isFeatureEnabled(SESSIONIZATION_FEATURE_NAME)) {
      configureAdTechAggregator(binder);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    ImmutableList.Builder<SimpleModule> jacksonModules = new ImmutableList.Builder<>();

    if (implyLicenseManager.isFeatureEnabled(SESSIONIZATION_FEATURE_NAME)) {
      log.info("The %s feature is enabled", SESSIONIZATION_FEATURE_NAME);
    }

    if (implyLicenseManager.isFeatureEnabled(SAMPLED_AVG_SCORE_FEATURE_NAME) ||
        implyLicenseManager.isFeatureEnabled(SESSIONIZATION_FEATURE_NAME)) {
      log.info("The %s feature is enabled", SAMPLED_AVG_SCORE_FEATURE_NAME);
      jacksonModules.add(sessiondAvgScoreJacksonModule());
    }
    if (implyLicenseManager.isFeatureEnabled(AD_TECH_AGGREGATORS_FEATURE_NAME) ||
        implyLicenseManager.isFeatureEnabled(SESSIONIZATION_FEATURE_NAME)) {
      log.info("The %s feature is enabled", AD_TECH_AGGREGATORS_FEATURE_NAME);
      jacksonModules.add(adTechAggregatorsJacksonModule());
    }
    return jacksonModules.build();
  }

  private void configureSessionAvgScore(Binder binder)
  {
    SqlBindings.addAggregator(binder, SessionAvgScoreObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToHistogramOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToEstimateOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToHistogramFilteringOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionFilterOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, MurmurHashOperatorConversions.Murmur3OperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, MurmurHashOperatorConversions.Murmur3_64OperatorConversion.class);
  }

  private void configureAdTechAggregator(Binder binder)
  {
    SqlBindings.addAggregator(binder, AdTechInventorySqlAggregator.class);
  }

  private SimpleModule sessiondAvgScoreJacksonModule()
  {
    return new SimpleModule(SESSIONIZATION_FEATURE_NAME).registerSubtypes(
        new NamedType(SessionAvgScoreAggregatorFactory.class, SESSION_AVG_SCORE),
        new NamedType(SessionAvgScoreToHistogramPostAggregator.class, SESSION_AVG_SCORE_HISTOGRAM),
        new NamedType(SessionAvgScoreToHistogramFilteringPostAggregator.class, SESSION_AVG_SCORE_HISTOGRAM_FILTERING),
        new NamedType(ImplySessionFilteringVirtualColumn.class, SESSION_FILTERING_VIRTUAL_COLUMN)
    );
  }

  private SimpleModule adTechAggregatorsJacksonModule()
  {
    return new SimpleModule(SESSIONIZATION_FEATURE_NAME).registerSubtypes(
        new NamedType(AdTechInventoryAggregatorFactory.class, AD_TECH_INVENTORY)
    );
  }
}
