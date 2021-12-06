/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.builtin.BuiltinApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;

public class BuiltinApproxCountDistinctRawSqlAggregator extends BuiltinApproxCountDistinctSqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ApproxCountDistinctRawSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT_BUILTIN_RAW";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final String actualAggName = Calcites.makePrefixedName(name, "a");

    final Aggregation aggregation = super.toDruidAggregation(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        rexBuilder,
        actualAggName,
        aggregateCall,
        project,
        existingAggregations,
        false
    );

    if (aggregation == null) {
      return null;
    }

    if (aggregation.getPostAggregator() != null) {
      throw new ISE("Expected null post-aggregator");
    }

    // Add a field-access post aggregator, which forces nonfinalized access.
    return Aggregation.create(
        aggregation.getAggregatorFactories(),
        new FieldAccessPostAggregator(name, actualAggName)
    );
  }

  private static class ApproxCountDistinctRawSqlAggFunction extends SqlAggFunction
  {
    ApproxCountDistinctRawSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(
              typeFactory ->
                  typeFactory.createTypeWithNullability(
                      new RowSignatures.ComplexSqlType(SqlTypeName.OTHER, HyperUniquesAggregatorFactory.TYPE, true),
                      true
                  )
          ),
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY,
          SqlFunctionCategory.STRING,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
