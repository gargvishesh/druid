/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris;

import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.stream.Collectors;

/**
 * SQL function to wrap {@link PolarisExplainTableMacro}
 */
public class PolarisExplainTableOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "POLARIS_EXPLAIN";
  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);
  private final SqlUserDefinedTableMacro operator;

  @Inject
  public PolarisExplainTableOperatorConversion(PolarisExplainTableMacro macro)
  {
    this.operator = new PolarisExplainTableOperator(macro);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return null;
  }

  private static class PolarisExplainTableOperator extends SqlUserDefinedTableMacro
  {
    public PolarisExplainTableOperator(PolarisExplainTableMacro macro)
    {
      super(
          new SqlIdentifier(FUNCTION_NAME, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          OperandTypes.family(SqlTypeFamily.STRING),
          macro.getParameters()
               .stream()
               .map(parameter -> parameter.getType(TYPE_FACTORY))
               .collect(Collectors.toList()),
          macro
      );
    }
  }
}
