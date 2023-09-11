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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesComplexMetricSerde;
import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.expression.ArithmeticTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.DeltaTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.IRRDebugOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.IRROverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.InterpolationTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.MaxOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.TimeWeightedAverageTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.TimeseriesSizeExprMacro;
import io.imply.druid.timeseries.expression.TimeseriesToJSONExprMacro;
import io.imply.druid.timeseries.sql.aggregation.DownsampledSumTimeSeriesObjectSqlAggregator;
import io.imply.druid.timeseries.sql.aggregation.IRRDebugOverTimeseriesObjectSqlAggregator;
import io.imply.druid.timeseries.sql.aggregation.IRROverTimeseriesObjectSqlAggregator;
import io.imply.druid.timeseries.sql.aggregation.SimpleTimeSeriesObjectSqlAggregator;
import io.imply.druid.timeseries.sql.aggregation.SumTimeSeriesObjectSqlAggregator;
import io.imply.druid.timeseries.sql.expression.ArithmeticTimeseriesOperatorConversion;
import io.imply.druid.timeseries.sql.expression.DeltaTimeseriesOperatorConversion;
import io.imply.druid.timeseries.sql.expression.InterpolationOperatorConversion;
import io.imply.druid.timeseries.sql.expression.MaxOverTimeseriesOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeWeightedAverageOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeseriesSizeOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeseriesToJSONOperatorConversion;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;

public class TimeSeriesModule implements DruidModule
{
  private static final String SIMPLE_TIMESERIES = "timeseries";
  private static final String DOWNSAMPLED_SUM_TIMESERIES = "downsampledSumTimeseries";

  public static final String SUM_TIMESERIES = "sumTimeseries";
  private final Logger log = new Logger(TimeSeriesModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("TimeSeriesModule").registerSubtypes(
            new NamedType(
                SimpleTimeSeriesAggregatorFactory.class,
                SIMPLE_TIMESERIES
            ),
            new NamedType(
                DownsampledSumTimeSeriesAggregatorFactory.class,
                DOWNSAMPLED_SUM_TIMESERIES
            ),
            new NamedType(
                SumTimeSeriesAggregatorFactory.class,
                SUM_TIMESERIES
            )
        ));
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();

    // add aggregators
    SqlBindings.addAggregator(binder, SimpleTimeSeriesObjectSqlAggregator.class);
    SqlBindings.addAggregator(binder, SumTimeSeriesObjectSqlAggregator.class);
    SqlBindings.addAggregator(binder, DownsampledSumTimeSeriesObjectSqlAggregator.class);
    SqlBindings.addAggregator(binder, IRROverTimeseriesObjectSqlAggregator.class);
    SqlBindings.addAggregator(binder, IRRDebugOverTimeseriesObjectSqlAggregator.class);

    // add post-processing bindings
    for (SqlOperatorConversion sqlOperatorConversion : InterpolationOperatorConversion.sqlOperatorConversionList()) {
      SqlBindings.addOperatorConversion(binder, sqlOperatorConversion.getClass());
    }
    SqlBindings.addOperatorConversion(binder, TimeWeightedAverageOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, MaxOverTimeseriesOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, TimeseriesToJSONOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, DeltaTimeseriesOperatorConversion.class);
    for (SqlOperatorConversion sqlOperatorConversion :
        ArithmeticTimeseriesOperatorConversion.sqlOperatorConversionList()) {
      SqlBindings.addOperatorConversion(binder, sqlOperatorConversion.getClass());
    }
    SqlBindings.addOperatorConversion(binder, TimeseriesSizeOperatorConversion.class);

    // add expressions
    ExpressionModule.addExprMacro(binder, MaxOverTimeseriesExprMacro.class);
    ExpressionModule.addExprMacro(binder, TimeseriesToJSONExprMacro.class);
    for (InterpolationTimeseriesExprMacro interpolationTimeseriesExprMacro : InterpolationTimeseriesExprMacro.getMacros()) {
      ExpressionModule.addExprMacro(binder, interpolationTimeseriesExprMacro.getClass());
    }
    ExpressionModule.addExprMacro(binder, TimeWeightedAverageTimeseriesExprMacro.class);
    ExpressionModule.addExprMacro(binder, DeltaTimeseriesExprMacro.class);
    for (ArithmeticTimeseriesExprMacro arithmeticTimeseriesExprMacro : ArithmeticTimeseriesExprMacro.getMacros()) {
      ExpressionModule.addExprMacro(binder, arithmeticTimeseriesExprMacro.getClass());
    }
    ExpressionModule.addExprMacro(binder, IRROverTimeseriesExprMacro.class);
    ExpressionModule.addExprMacro(binder, IRRDebugOverTimeseriesExprMacro.class);
    ExpressionModule.addExprMacro(binder, TimeseriesSizeExprMacro.class);
  }

  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(
        SimpleTimeSeriesComplexMetricSerde.TYPE_NAME,
        new SimpleTimeSeriesComplexMetricSerde()
    );
  }
}
