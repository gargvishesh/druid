/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.stringmatch;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class StringMatchSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION =
      OperatorConversions.aggregatorBuilder("STRING_MATCH")
                         .operandNames("string", "maxSizeBytes")
                         .operandTypes(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)
                         .requiredOperandCount(1)
                         .literalOperands(1)
                         .returnTypeNullable(SqlTypeName.VARCHAR)
                         .build();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final List<RexNode> arguments = inputAccessor.getFields(aggregateCall.getArgList());

    Integer maxSizeBytes = null;
    if (arguments.size() > 1) {
      RexNode maxBytes = arguments.get(1);
      maxSizeBytes = ((Number) RexLiteral.value(maxBytes)).intValue();
    }

    final DruidExpression arg = Expressions.toDruidExpression(
        plannerContext,
        inputAccessor.getInputRowSignature(),
        arguments.get(0)
    );
    if (arg == null) {
      return null;
    }

    final String fieldName;

    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          arg,
          Calcites.getColumnTypeForRelDataType(aggregateCall.getType())
      );
    }

    return Aggregation.create(new StringMatchAggregatorFactory(name, fieldName, maxSizeBytes));
  }
}
