/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.aggregation.datasketches.tuple.SampledAvgScoreAggregatorFactory;
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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class SampledAvgScoreObjectSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new SampledAvgScoreSqlAggFunction();
  private static final String NAME = "SAMPLED_AVG_SCORE";

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
    String sampleColumnName, scoreColumnName;

    // fetch sample column name
    RexNode node = Expressions.fromFieldAccess(rowSignature, project, aggregateCall.getArgList().get(0));
    DruidExpression sampleColumn = Expressions.toDruidExpression(plannerContext, rowSignature, node);
    if (sampleColumn == null) {
      return null;
    }
    if (sampleColumn.isDirectColumnAccess()) {
      sampleColumnName = sampleColumn.getDirectColumn();
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          sampleColumn,
          node.getType()
      );
      sampleColumnName = virtualColumn.getOutputName();
    }

    // fetch score column name
    DruidExpression scoreColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rowSignature,
            project,
            aggregateCall.getArgList().get(1)
        )
    );
    if (scoreColumn == null) {
      return null;
    }
    if (scoreColumn.isDirectColumnAccess()) {
      scoreColumnName = scoreColumn.getDirectColumn();
    } else {
      VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          plannerContext,
          scoreColumn,
          ValueType.DOUBLE
      );
      scoreColumnName = virtualColumn.getOutputName();
    }

    // check if target sample size is provided
    int targetSamples;
    if (aggregateCall.getArgList().size() == 3) {
      final RexNode targetSamplesArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );

      if (!targetSamplesArg.isA(SqlKind.LITERAL)) {
        // target samples must be a literal in order to plan.
        return null;
      }

      targetSamples = ((Number) RexLiteral.value(targetSamplesArg)).intValue();
    } else {
      targetSamples = SampledAvgScoreAggregatorFactory.DEFAULT_TARGET_SAMPLES;
    }

    // create the factory
    AggregatorFactory aggregatorFactory = SampledAvgScoreAggregatorFactory.getAvgSessionScoresAggregatorFactory(
        StringUtils.format("%s:agg", name),
        sampleColumnName,
        scoreColumnName,
        targetSamples);

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
  }

  private static class SampledAvgScoreSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "'(sampleColumn, scoreColumn)";
    private static final String SIGNATURE2 = "'" + NAME + "'(sampleColumn, scoreColumn, targetSamples)";

    SampledAvgScoreSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.OTHER),
          null,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY, OperandTypes.ANY),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY)),
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.ANY, OperandTypes.POSITIVE_INTEGER_LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.EXACT_NUMERIC)
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
