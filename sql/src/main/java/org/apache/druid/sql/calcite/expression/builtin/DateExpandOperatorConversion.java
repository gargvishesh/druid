package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Function;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class DateExpandOperatorConversion extends DirectOperatorConversion
{
  // note: since this function produces an array
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(Function.DateExpandFunction.OPERATOR_NAME))
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
      .requiredOperands(3)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .returnTypeNullableArrayWithNullableElements(SqlTypeName.TIMESTAMP)
      .build();

  public DateExpandOperatorConversion()
  {
    super(SQL_FUNCTION, Function.DateExpandFunction.OPERATOR_NAME);
  }
}
