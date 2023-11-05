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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesContainerComplexMetricSerde;
import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.expression.ArithmeticTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.DeltaTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.IRRDebugOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.IRROverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.InterpolationTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.MaxOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.QuantileOverTimeseriesExprMacro;
import io.imply.druid.timeseries.expression.SumOverTimeseriesExprMacro;
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
import io.imply.druid.timeseries.sql.expression.QuantilesOverTimeseriesOperatorConversion;
import io.imply.druid.timeseries.sql.expression.SumOverTimeseriesOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeWeightedAverageOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeseriesSizeOperatorConversion;
import io.imply.druid.timeseries.sql.expression.TimeseriesToJSONOperatorConversion;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
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
    SqlBindings.addOperatorConversion(binder, SumOverTimeseriesOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, QuantilesOverTimeseriesOperatorConversion.class);

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
    ExpressionModule.addExprMacro(binder, SumOverTimeseriesExprMacro.class);
    ExpressionModule.addExprMacro(binder, QuantileOverTimeseriesExprMacro.class);
  }

  public static void registerSerde()
  {
    ComplexMetrics.registerSerde(
        SimpleTimeSeriesContainerComplexMetricSerde.TYPE_NAME,
        new SimpleTimeSeriesContainerComplexMetricSerde()
    );
    ComplexMetrics.registerSerde(
        TimeseriesToJSONExprMacro.TYPE.getComplexTypeName(),
        new NoSerde(TimeseriesToJSONExprMacro.TYPE)
    );
    ComplexMetrics.registerSerde(
        IRROverTimeseriesExprMacro.IRR_DEBUG_TYPE.getComplexTypeName(),
        new NoSerde(IRROverTimeseriesExprMacro.IRR_DEBUG_TYPE)
    );
  }

  private static class NoSerde extends ComplexMetricSerde
  {
    private final ColumnType type;

    NoSerde(ColumnType columnType)
    {
      this.type = columnType;
    }

    @Override
    public String getTypeName()
    {
      return type.getComplexTypeName();
    }

    @Override
    public ComplexMetricExtractor getExtractor()
    {
      throw DruidException.defensive("Type[%s] does not support ComplexMetricExtractor", type);
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
    {
      throw DruidException.defensive("Type[%s] cannot be deserialized from a column", type);
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy()
      {
        @Override
        public Class getClazz()
        {
          throw DruidException.defensive("Type[%s] is not associated with a Class", type);
        }

        @Nullable
        @Override
        public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          throw DruidException.defensive("Type[%s] cannot be read from a ByteBuffer", type);
        }

        @Nullable
        @Override
        public byte[] toBytes(@Nullable Object val)
        {
          throw DruidException.defensive("Type[%s] cannot be serialized to a byte array", type);
        }

        @Override
        public int compare(Object o1, Object o2)
        {
          throw DruidException.defensive("Type[%s] does not support comparison", type);
        }
      };
    }
  }
}
