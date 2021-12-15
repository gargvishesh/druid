/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import io.imply.druid.query.aggregation.datasketches.expressions.MurmurHashExprMacros;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class MurmurHashOperatorConversions
{
  public static class Murmur3OperatorConversion extends DirectOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(MurmurHashExprMacros.Murmur3Macro.NAME))
        .operandTypeChecker(OperandTypes.ANY)
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.VARCHAR))
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();

    public Murmur3OperatorConversion()
    {
      super(SQL_FUNCTION, MurmurHashExprMacros.Murmur3Macro.NAME);
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }

  public static class Murmur3_64OperatorConversion extends DirectOperatorConversion
  {
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(MurmurHashExprMacros.Murmur3_64Macro.NAME))
        .operandTypeChecker(OperandTypes.ANY)
        .returnTypeInference(ReturnTypes.explicit(SqlTypeName.BIGINT))
        .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
        .build();

    public Murmur3_64OperatorConversion()
    {
      super(SQL_FUNCTION, MurmurHashExprMacros.Murmur3_64Macro.NAME);
    }

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }
  }
}
