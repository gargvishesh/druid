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
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesContainerComplexMetricSerde;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SimpleTimeSeriesAggregatorFactory;
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
import org.apache.druid.query.aggregation.AggregatorFactory;
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

import javax.annotation.Nullable;
import java.util.List;

public class SimpleTimeSeriesObjectSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SimpleTimeSeriesSqlAggFunction();
  private static final String NAME = "TIMESERIES";

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
    if (aggregateCall.getArgList().size() < 2) {
      return null;
    }

    String timeseriesColumnName = null, timeColumnName = null, dataColumnName = null;
    int argCounter = 0;
    if (aggregateCall.getArgList().size() == 2 ||
        // timeseries column case
        (aggregateCall.getArgList().size() == 3 &&
         isFieldMaxEntriesArg(
             aggregateCall.getArgList().get(2),
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

    // check if maxEntries is provided
    int maxEntries;
    if (aggregateCall.getArgList().size() == 4 ||
        // timeseries column case
        (aggregateCall.getArgList().size() == 3 &&
         isFieldMaxEntriesArg(
             aggregateCall.getArgList().get(2),
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
    } else {
      maxEntries = SimpleTimeSeriesAggregatorFactory.DEFAULT_MAX_ENTRIES;
    }

    // create the factory
    AggregatorFactory aggregatorFactory = SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
        StringUtils.format("%s:agg", name),
        dataColumnName,
        timeColumnName,
        timeseriesColumnName,
        window,
        maxEntries);

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
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

  private static class SimpleTimeSeriesSqlAggFunction extends SqlAggFunction
  {
    SimpleTimeSeriesSqlAggFunction()
    {
      super(
          NAME,
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
                  "'" + NAME + "'(timeColumn, dataColumn, window)",
                  OperandTypes.ANY,
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING)
              ),
              OperandTypes.sequence(
                  "'" + NAME + "'(timeColumn, dataColumn, window, maxEntries)",
                  OperandTypes.ANY,
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING),
                  OperandTypes.POSITIVE_INTEGER_LITERAL
              ),
              OperandTypes.sequence(
                  "'" + NAME + "'(timeseriesColumn, window)",
                  OperandTypes.ANY,
                  OperandTypes.and(OperandTypes.LITERAL, OperandTypes.STRING)
              ),
              OperandTypes.sequence(
                  "'" + NAME + "'(timeseriesColumn, window, maxEntries)",
                  OperandTypes.ANY,
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
