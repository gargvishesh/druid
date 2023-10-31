/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql.aggregation;

import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesContainerComplexMetricSerde;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.DownsampledSumTimeSeriesAggregatorFactory;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
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
    if (aggregateCall.getArgList().size() < 3) {
      return null;
    }

    // timeseries column
    String timeseriesColumnName = null, timeColumnName = null, dataColumnName = null;
    int argCounter = 0;
    if (aggregateCall.getArgList().size() == 3 ||
        // timeseries column case
        (aggregateCall.getArgList().size() == 4 &&
         isFieldMaxEntriesArg(
             aggregateCall.getArgList().get(3),
             rexBuilder.getTypeFactory(),
             rowSignature,
             project
         )
        )
    ) {
      // fetch timeseries column name
      DruidExpression timeseriesColumn = Aggregations.toDruidExpressionForNumericAggregator(
          plannerContext,
          rowSignature,
          Expressions.fromFieldAccess(
              rexBuilder.getTypeFactory(),
              rowSignature,
              project,
              aggregateCall.getArgList().get(argCounter)
          )
      );
      if (timeseriesColumn == null) {
        return null;
      }
      timeseriesColumnName = extractColumnNameFromExpression(
          timeseriesColumn,
          virtualColumnRegistry,
          plannerContext,
          ColumnType.ofComplex(SimpleTimeSeriesContainerComplexMetricSerde.TYPE_NAME)
      );
      argCounter++;
    } else {
      // fetch time column name
      DruidExpression timeColumn = Aggregations.toDruidExpressionForNumericAggregator(
          plannerContext,
          rowSignature,
          Expressions.fromFieldAccess(
              rexBuilder.getTypeFactory(),
              rowSignature,
              project,
              aggregateCall.getArgList().get(argCounter)
          )
      );
      if (timeColumn == null) {
        return null;
      }
      timeColumnName = extractColumnNameFromExpression(
          timeColumn,
          virtualColumnRegistry,
          plannerContext,
          ColumnType.LONG
      );
      argCounter++;

      // fetch data column name
      DruidExpression dataColumn = Aggregations.toDruidExpressionForNumericAggregator(
          plannerContext,
          rowSignature,
          Expressions.fromFieldAccess(
              rexBuilder.getTypeFactory(),
              rowSignature,
              project,
              aggregateCall.getArgList().get(argCounter)
          )
      );
      if (dataColumn == null) {
        return null;
      }
      dataColumnName = extractColumnNameFromExpression(
          dataColumn,
          virtualColumnRegistry,
          plannerContext,
          ColumnType.DOUBLE
      );
      argCounter++;
    }

    // fetch time window
    final RexNode timeWindow = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(argCounter)
    );
    Interval window = Intervals.of(RexLiteral.stringValue(timeWindow));
    argCounter++;

    // fetch bucket millis
    final RexNode timeBucketMillisArg = Expressions.fromFieldAccess(
        rexBuilder.getTypeFactory(),
        rowSignature,
        project,
        aggregateCall.getArgList().get(argCounter)
    );
    long timeBucketMillis = new Period(RexLiteral.stringValue(timeBucketMillisArg)).toStandardDuration().getMillis();
    argCounter++;

    // check if maxEntries is provided
    Integer maxEntries = null;
    if (aggregateCall.getArgList().size() == 5 ||
        // timeseries column case
        (aggregateCall.getArgList().size() == 4 &&
         isFieldMaxEntriesArg(
             aggregateCall.getArgList().get(3),
             rexBuilder.getTypeFactory(),
             rowSignature,
             project
         )
        )
    ) {
      final RexNode maxEntriesArg = Expressions.fromFieldAccess(
          rexBuilder.getTypeFactory(),
          rowSignature,
          project,
          aggregateCall.getArgList().get(argCounter)
      );
      maxEntries = ((Number) RexLiteral.value(maxEntriesArg)).intValue();
    }

    // create the factory
    return Aggregation.create(
        DownsampledSumTimeSeriesAggregatorFactory.getDownsampledSumTimeSeriesAggregationFactory(
            StringUtils.format("%s:agg", name),
            dataColumnName,
            timeColumnName,
            timeseriesColumnName,
            timeBucketMillis,
            window,
            maxEntries
        )
    );
  }

  private String extractColumnNameFromExpression(
      DruidExpression expression,
      VirtualColumnRegistry virtualColumnRegistry,
      PlannerContext plannerContext,
      ColumnType columnType
  )
  {
    if (expression.isDirectColumnAccess()) {
      return expression.getDirectColumn();
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          expression,
          columnType
      );
      return virtualColumn.getOutputName();
    }
  }

  private boolean isFieldMaxEntriesArg(
      int fieldNumber,
      RelDataTypeFactory typeFactory,
      RowSignature rowSignature,
      Project project
  )
  {
    try {
      final RexNode maxEntriesArg = Expressions.fromFieldAccess(
          typeFactory,
          rowSignature,
          project,
          fieldNumber
      );
      ((Number) RexLiteral.value(maxEntriesArg)).intValue();
      return true;
    }
    catch (Exception e) {
      return false;
    }
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
              OperandTypes.sequence(
                  "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeColumn, dataColumn, window, bucketPeriod)",
                  OperandTypes.ANY,
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING)
              ),
              OperandTypes.sequence(
                  "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeColumn, dataColumn, window, bucketPeriod, maxEntries)",
                  OperandTypes.ANY,
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.POSITIVE_INTEGER_LITERAL
              ),
              OperandTypes.sequence(
                  "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeseriesColumn, window, bucketPeriod)",
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING)
              ),
              OperandTypes.sequence(
                  "'" + DOWNSAMPLED_SUM_TIMESERIES + "'(timeseriesColumn, window, bucketPeriod, maxEntries)",
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.POSITIVE_INTEGER_LITERAL
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
