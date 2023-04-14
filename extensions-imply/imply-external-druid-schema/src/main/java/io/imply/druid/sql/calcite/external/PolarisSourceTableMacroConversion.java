/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.DruidTableMacro;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

public class PolarisSourceTableMacroConversion implements SqlOperatorConversion
{
  private final SqlUserDefinedTableMacro operator;

  public PolarisSourceTableMacroConversion(
      final String name,
      final TableFunction fn,
      final ObjectMapper jsonMapper
  )
  {
    this.operator = new PolarisSourceTableMacro(
        new DruidTableMacro(name, fn, jsonMapper)
    );
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
}
