/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.google.inject.Inject;
import io.imply.druid.talaria.indexing.TalariaControllerTask;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.ExternalTableMacro;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Copy of {@link org.apache.druid.sql.calcite.external.ExternalOperatorConversion} that has a different value
 * for {@link #EXTERNAL_RESOURCE_ACTION}.
 */
public class TalariaExternalOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "EXTERN";

  public static final ResourceAction EXTERNAL_RESOURCE_ACTION =
      new ResourceAction(
          new Resource(TalariaControllerTask.DUMMY_DATASOURCE_FOR_SELECT, ResourceType.DATASOURCE),
          Action.WRITE
      );

  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  private final SqlUserDefinedTableMacro operator;

  @Inject
  public TalariaExternalOperatorConversion(final ExternalTableMacro macro)
  {
    this.operator = new ExternalOperator(macro);
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

  private static class ExternalOperator extends SqlUserDefinedTableMacro implements AuthorizableOperator
  {
    public ExternalOperator(final ExternalTableMacro macro)
    {
      super(
          new SqlIdentifier(FUNCTION_NAME, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          OperandTypes.sequence(
              "(inputSource, inputFormat, signature)",
              OperandTypes.family(SqlTypeFamily.STRING),
              OperandTypes.family(SqlTypeFamily.STRING),
              OperandTypes.family(SqlTypeFamily.STRING)
          ),
          macro.getParameters()
               .stream()
               .map(parameter -> parameter.getType(TYPE_FACTORY))
               .collect(Collectors.toList()),
          macro
      );
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(EXTERNAL_RESOURCE_ACTION);
    }
  }
}
