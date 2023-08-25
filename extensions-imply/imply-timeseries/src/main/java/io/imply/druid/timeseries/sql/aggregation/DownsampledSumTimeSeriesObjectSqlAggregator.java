/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.aggregation;

import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;

public class DownsampledSumTimeSeriesObjectSqlAggregator implements SqlAggregator
{
  public static final String DOWNSAMPLED_SUM_TIMESERIES = "DOWNSAMPLED_SUM_TIMESERIES";

  private static final SqlAggFunction FUNCTION = new DownsampledSumTimeSeriesSqlAggFunction();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION;
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
    if (aggregateCall.getArgList().size() < 4) {
      return null;
    }

    String timeColumnName, dataColumnName;
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
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          timeColumn,
          ColumnType.LONG
      );
      timeColumnName = virtualColumn.getOutputName();
    }

    // fetch data column name
    DruidExpression dataColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rexBuilder.getTypeFactory(),
            rowSignature,
            project,
            aggregateCall.getArgList().get(1)
        )
    );
    if (dataColumn == null) {
      return null;
    }
    if (dataColumn.isDirectColumnAccess()) {
      dataColumnName = dataColumn.getDirectColumn();
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          dataColumn,
          ColumnType.DOUBLE
      );
      dataColumnName = virtualColumn.getOutputName();
    }

    // fetch time window
    final RexNode timeWindow = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(2)
    );
    Interval window = Intervals.of(RexLiteral.stringValue(timeWindow));

    final RexNode timeBucketMillisArg = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(3)
    );
    long timeBucketMillis = new Period(RexLiteral.stringValue(timeBucketMillisArg)).toStandardDuration().getMillis();

    // check if maxEntries is provided
    Integer maxEntries = null;
    if (aggregateCall.getArgList().size() == 5) {
      final RexNode maxEntriesArg = Expressions.fromFieldAccess(
          rexBuilder.getTypeFactory(),
          rowSignature,
          project,
          aggregateCall.getArgList().get(4)
      );
      maxEntries = ((Number) RexLiteral.value(maxEntriesArg)).intValue();
    }

    // create the factory
    return Aggregation.create(
        DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
            StringUtils.format("%s:agg", name),
            dataColumnName,
            timeColumnName,
            null,
            timeBucketMillis,
            window,
            maxEntries
        )
    );
  }

  private static class DownsampledSumTimeSeriesSqlAggFunction extends SqlAggFunction
  {
    DownsampledSumTimeSeriesSqlAggFunction()
    {
      super(
          DOWNSAMPLED_SUM_TIMESERIES,
          null,
          SqlKind.OTHER_FUNCTION,
          opBinding -> RowSignatures.makeComplexType(
              opBinding.getTypeFactory(),
              BaseTimeSeriesAggregatorFactory.TYPE,
              true
          ),
          null,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(
                      "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeColumn, dataColumn, window, bucketPeriod)",
                      OperandTypes.ANY,
                      OperandTypes.ANY,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.STRING,
                      SqlTypeFamily.STRING
                  )
              ),
              OperandTypes.and(
                  OperandTypes.sequence(
                      "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeColumn, dataColumn, window, bucketPeriod, maxEntries)",
                      OperandTypes.ANY,
                      OperandTypes.ANY,
                      OperandTypes.LITERAL,
                      OperandTypes.LITERAL,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.STRING,
                      SqlTypeFamily.STRING,
                      SqlTypeFamily.EXACT_NUMERIC
                  )
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