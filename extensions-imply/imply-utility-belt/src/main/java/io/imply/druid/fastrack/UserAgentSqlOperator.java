/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.fastrack;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class UserAgentSqlOperator extends DirectOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("FT_USERAGENT")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .returnTypeNullable(SqlTypeName.VARCHAR)
      .functionCategory(SqlFunctionCategory.STRING)
      .build();

  public UserAgentSqlOperator()
  {
    super(SQL_FUNCTION, StringUtils.toLowerCase(SQL_FUNCTION.getName()));
  }
}
