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
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.aggregation.SumTimeSeriesAggregatorFactory;
import io.imply.druid.timeseries.sql.TypeUtils;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.util.Optionality;
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

import javax.annotation.Nullable;
import java.util.List;

public class SumTimeSeriesObjectSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SumTimeSeriesSqlAggFunction();
  private static final String NAME = "SUM_TIMESERIES";
  public static final int SQL_DEFAULT_MAX_ENTRIES = 260_000;

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
    if (aggregateCall.getArgList().size() < 1) {
      return null;
    }

    String timeSeriesColumnName;
    // fetch time series column name
    DruidExpression timeSeriesColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rexBuilder.getTypeFactory(),
            rowSignature,
            project,
            aggregateCall.getArgList().get(0)
        )
    );
    if (timeSeriesColumn == null) {
      return null;
    }
    if (timeSeriesColumn.isDirectColumnAccess()) {
      timeSeriesColumnName = timeSeriesColumn.getDirectColumn();
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          timeSeriesColumn,
          ColumnType.LONG
      );
      timeSeriesColumnName = virtualColumn.getOutputName();
    }

    // check if maxEntries is provided
    int maxEntries;
    if (aggregateCall.getArgList().size() == 2) {
      final RexNode maxEntriesArg = Expressions.fromFieldAccess(
          rexBuilder.getTypeFactory(),
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );
      if (!maxEntriesArg.isA(SqlKind.LITERAL)) {
        // In some cases, even if the maxEntries parameter is
        // present as a literal, the child node of an aggregate may not be a LogicalProject through which the constant can
        // be extracted. In such cases, the parameter becomes an RexInputRef for the aggreate leading to query planning
        // failure. A test has been added for the same (testSumTimeseriesAggOuterQuery_MaxEntriesParameterFailure), so
        // that when we fix this problem, we can remove that test as well.
        return null;
      } else {
        maxEntries = ((Number) RexLiteral.value(maxEntriesArg)).intValue();
      }
    } else {
      // keeping the default as a large number due to the above problem where sometimes the maxEntries parameters is
      // unprocessable due to the current integration with Calcite
      maxEntries = SQL_DEFAULT_MAX_ENTRIES;
    }

    // create the factory
    AggregatorFactory aggregatorFactory = SumTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
        StringUtils.format("%s:agg", name),
        timeSeriesColumnName,
        maxEntries
    );

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
  }

  private static class SumTimeSeriesSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE2 = "'" + NAME + "'(timeSeriesColumn, maxEntries)";

    SumTimeSeriesSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          opBinding -> RowSignatures.makeComplexType(
              opBinding.getTypeFactory(),
              SumTimeSeriesAggregatorFactory.TYPE,
              true
          ),
          null,
          OperandTypes.or(
              TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
              OperandTypes.sequence(
                  SIGNATURE2,
                  TypeUtils.complexTypeChecker(BaseTimeSeriesAggregatorFactory.TYPE),
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
