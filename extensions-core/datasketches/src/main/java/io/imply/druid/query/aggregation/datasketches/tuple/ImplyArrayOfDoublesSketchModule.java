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
import io.imply.druid.query.aggregation.datasketches.expressions.MurmurHashExprMacros;
import io.imply.druid.query.aggregation.datasketches.expressions.SessionizeExprMacro;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.AdTechInventorySqlAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.MurmurHashOperatorConversions;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreObjectSqlAggregator;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreSummaryStatsOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToEstimateOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToHistogramFilteringOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionAvgScoreToHistogramOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionFilterOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionSampleRateOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.tuple.sql.SessionScoreSummaryStatsOperatorConversion;
import io.imply.druid.query.aggregation.datasketches.virtual.ImplySessionFilteringVirtualColumn;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class ImplyArrayOfDoublesSketchModule implements DruidModule
{
  static final String AD_TECH_INVENTORY = "adTechInventory";
  static final String SESSION_AVG_SCORE = "sessionAvgScore";
  static final String SESSION_AVG_SCORE_HISTOGRAM = "sessionAvgScoreHistogram";
  static final String SESSION_AVG_SCORE_HISTOGRAM_FILTERING = "sessionAvgScoreHistogramFiltering";
  static final String SESSION_SAMPLE_RATE = "sessionSampleRate";
  public static final String SESSIONIZATION_FEATURE_NAME = "sessionization";
  public static final String SESSION_FILTERING_VIRTUAL_COLUMN = "session-filtering";

  @Override
  public void configure(Binder binder)
  {
    ExpressionModule.addExprMacro(binder, MurmurHashExprMacros.Murmur3Macro.class);
    ExpressionModule.addExprMacro(binder, MurmurHashExprMacros.Murmur3_64Macro.class);

    SqlBindings.addOperatorConversion(binder, MurmurHashOperatorConversions.Murmur3OperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, MurmurHashOperatorConversions.Murmur3_64OperatorConversion.class);

    configureSessionAvgScore(binder);
    ExpressionModule.addExprMacro(binder, SessionizeExprMacro.class);

    configureAdTechAggregator(binder);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    ImmutableList.Builder<SimpleModule> jacksonModules = new ImmutableList.Builder<>();
    jacksonModules.add(sessionAvgScoreJacksonModule());
    jacksonModules.add(adTechAggregatorsJacksonModule());
    return jacksonModules.build();
  }

  private void configureSessionAvgScore(Binder binder)
  {
    SqlBindings.addAggregator(binder, SessionAvgScoreObjectSqlAggregator.class);

    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToHistogramOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToEstimateOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionAvgScoreToHistogramFilteringOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionFilterOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionSampleRateOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionScoreSummaryStatsOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, SessionAvgScoreSummaryStatsOperatorConversion.class);
  }

  private void configureAdTechAggregator(Binder binder)
  {
    SqlBindings.addAggregator(binder, AdTechInventorySqlAggregator.class);
  }

  private SimpleModule sessionAvgScoreJacksonModule()
  {
    return new SimpleModule(SESSIONIZATION_FEATURE_NAME).registerSubtypes(
        new NamedType(SessionAvgScoreAggregatorFactory.class, SESSION_AVG_SCORE),
        new NamedType(SessionAvgScoreToHistogramPostAggregator.class, SESSION_AVG_SCORE_HISTOGRAM),
        new NamedType(SessionAvgScoreSummaryStatsPostAggregator.class, "sessionAvgScoreStats"),
        new NamedType(SessionAvgScoreToHistogramFilteringPostAggregator.class, SESSION_AVG_SCORE_HISTOGRAM_FILTERING),
        new NamedType(SessionSampleRatePostAggregator.class, SESSION_SAMPLE_RATE),
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
