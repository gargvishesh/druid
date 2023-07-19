/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.aggregation;

import com.google.common.collect.ImmutableList;
import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;

public class IRROverTimeseriesObjectSqlAggregator implements SqlAggregator
{

  private static final String NAME = "IRR";
  private static final SqlAggFunction FUNCTION_INSTANCE = new IRROverTimeSeriesSqlAggFunction(NAME, SqlTypeName.DOUBLE);

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    if (aggregateCall.getArgList().size() < 5) {
      return null;
    }

    String timeColumnName;
    // fetch time column name
    DruidExpression timeColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rexBuilder.getTypeFactory(),
            rowSignature,
            project,
            aggregateCall.getArgList().get(0)
        )
    );
    if (timeColumn == null) {
      return null;
    }
    if (timeColumn.isDirectColumnAccess()) {
      timeColumnName = timeColumn.getDirectColumn();
    } else {
      timeColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          timeColumn,
          ColumnType.LONG
      );
    }

    String cashFlowColumnName, aumColumnName;
    // fetch cashFlow column name
    DruidExpression cashFlowColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rexBuilder.getTypeFactory(),
            rowSignature,
            project,
            aggregateCall.getArgList().get(1)
        )
    );
    if (cashFlowColumn == null) {
      return null;
    }
    if (cashFlowColumn.isDirectColumnAccess()) {
      cashFlowColumnName = cashFlowColumn.getDirectColumn();
    } else {
      cashFlowColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          cashFlowColumn,
          ColumnType.LONG
      );
    }

    // fetch aum column name
    DruidExpression aumColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rexBuilder.getTypeFactory(),
            rowSignature,
            project,
            aggregateCall.getArgList().get(2)
        )
    );
    if (aumColumn == null) {
      return null;
    }
    if (aumColumn.isDirectColumnAccess()) {
      aumColumnName = aumColumn.getDirectColumn();
    } else {
      aumColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          aumColumn,
          ColumnType.DOUBLE
      );
    }

    // fetch time window
    final RexNode timeWindow = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(3)
    );
    Interval window = Intervals.of(RexLiteral.stringValue(timeWindow));

    // fetch bucket millis
    final RexNode timeBucketMillisArg = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(4)
    );
    Period timeBucketPeriod = new Period(RexLiteral.stringValue(timeBucketMillisArg));

    Double initEstimate = null;
    if (aggregateCall.getArgList().size() == 6) {
      final RexNode initEstimateNode = Expressions.fromFieldAccess(
          rexBuilder.getTypeFactory(),
          rowSignature,
          project,
          aggregateCall.getArgList().get(5)
      );
      initEstimate = Double.parseDouble(RexLiteral.stringValue(initEstimateNode));
    }

    DurationGranularity durationGranularity = new DurationGranularity(
        timeBucketPeriod.toStandardDuration().getMillis(),
        0
    );
    if (!durationGranularity.bucketStart(window.getStart()).equals(window.getStart()) ||
        !durationGranularity.bucketStart(window.getEnd()).equals(window.getEnd())) {
      throw InvalidSqlInput.exception(
          "Please ensure that the time window [%s] input aligns with the time bucket [%s] input",
          window,
          timeBucketPeriod
      );
    }

    DruidExpression timeFloorVirtualColumn = DruidExpression.ofFunctionCall(
        ColumnType.LONG,
        "timestamp_floor",
        ImmutableList.of(
            DruidExpression.ofColumn(ColumnType.LONG, timeColumnName),
            DruidExpression.ofStringLiteral(timeBucketPeriod.toString())
        )
    );
    String timeFloorVirtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
        timeFloorVirtualColumn,
        ColumnType.LONG
    );
    String downsampledSumColumn = Calcites.makePrefixedName(name, "downsampledSum");
    AggregatorFactory downsampledSumTimeSeriesAggregationFactory =
        DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
            downsampledSumColumn,
            cashFlowColumnName,
            timeColumnName,
            null,
            timeBucketPeriod.toStandardDuration().getMillis(),
            window,
            null
        );
    String startInvestmentAgg = Calcites.makePrefixedName(name, "startInvestment");
    AggregatorFactory startInvestmentAggregationFactory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory(Calcites.makePrefixedName(name, "startInvestmentInternal"), aumColumnName),
        plannerContext.isUseBoundsAndSelectors()
        ? new SelectorDimFilter(
            timeFloorVirtualColumnName,
            Long.toString(window.getStartMillis()),
            null
        )
        : new EqualityFilter(
            timeFloorVirtualColumnName,
            ColumnType.LONG,
            window.getStartMillis(),
            null,
            null
        ),
        startInvestmentAgg
    );
    String endInvestmentAgg = Calcites.makePrefixedName(name, "endInvestment");
    AggregatorFactory endInvestmentAggregationFactory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory(Calcites.makePrefixedName(name, "endInvestmentInternal"), aumColumnName),
        plannerContext.isUseBoundsAndSelectors()
        ? new SelectorDimFilter(
            timeFloorVirtualColumnName,
            Long.toString(window.getEndMillis()),
            null
        )
        : new EqualityFilter(
            timeFloorVirtualColumnName,
            ColumnType.LONG,
            window.getEndMillis(),
            null,
            null
        ),
        endInvestmentAgg
    );

    return Aggregation.create(
        ImmutableList.of(
            downsampledSumTimeSeriesAggregationFactory,
            startInvestmentAggregationFactory,
            endInvestmentAggregationFactory
        ),
        new ExpressionPostAggregator(
            name,
            buildPostAggregator(
                getPostAggregatorName(),
                downsampledSumColumn,
                startInvestmentAgg,
                endInvestmentAgg,
                window,
                initEstimate
            ),
            null,
            plannerContext.getPlannerToolbox().exprMacroTable()
        )
    );
  }

  public String getPostAggregatorName()
  {
    return NAME;
  }

  private String buildPostAggregator(
      String functionName,
      String downsampledSumColumn,
      String startInvestmentAgg,
      String endInvestmentAgg,
      Interval window,
      @Nullable Double initEstimate
  )
  {
    if (initEstimate != null) {
      return StringUtils.format(
          "%s(\"%s\",\"%s\",\"%s\",'%s',%s)",
          functionName,
          downsampledSumColumn,
          startInvestmentAgg,
          endInvestmentAgg,
          window,
          initEstimate
      );
    } else {
      return StringUtils.format(
          "%s(\"%s\",\"%s\",\"%s\",'%s')",
          functionName,
          downsampledSumColumn,
          startInvestmentAgg,
          endInvestmentAgg,
          window
      );
    }
  }

  public static class IRROverTimeSeriesSqlAggFunction extends SqlAggFunction
  {

    public IRROverTimeSeriesSqlAggFunction(String name, SqlTypeName typeName)
    {
      super(
          name,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(typeName),
          null,
          OperandTypes.or(
              OperandTypes.sequence(
                  "'" + name + "'(time, cashFlow, aum, window, bucketPeriod)",
                  OperandTypes.or(OperandTypes.DATETIME, OperandTypes.NUMERIC),
                  OperandTypes.NUMERIC,
                  OperandTypes.NUMERIC,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING)
              ),
              OperandTypes.sequence(
                  "'" + name + "'(time, cashFlow, aum, window, bucketPeriod, initEstimate)",
                  OperandTypes.or(OperandTypes.DATETIME, OperandTypes.NUMERIC),
                  OperandTypes.NUMERIC,
                  OperandTypes.NUMERIC,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.NUMERIC)
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}
