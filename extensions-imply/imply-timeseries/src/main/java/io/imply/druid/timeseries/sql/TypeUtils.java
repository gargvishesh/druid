/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.table.RowSignatures;

public class TypeUtils
{
  public static ComplexSqlSingleOperandTypeChecker complexTypeChecker(ColumnType complexType)
  {
    return new ComplexSqlSingleOperandTypeChecker(
        new RowSignatures.ComplexSqlType(SqlTypeName.OTHER, complexType, true)
    );
  }

  public static final class ComplexSqlSingleOperandTypeChecker implements SqlSingleOperandTypeChecker
  {
    private final RowSignatures.ComplexSqlType type;

    public ComplexSqlSingleOperandTypeChecker(
        RowSignatures.ComplexSqlType type
    )
    {
      this.type = type;
    }

    @Override
    public boolean checkSingleOperandType(
        SqlCallBinding callBinding,
        SqlNode operand,
        int iFormalOperand,
        boolean throwOnFailure
    )
    {
      return type.equals(callBinding.getValidator().deriveType(callBinding.getScope(), operand));
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
    {
      if (callBinding.getOperandCount() != 1) {
        return false;
      }
      return checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange()
    {
      return SqlOperandCountRanges.of(1);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
      return StringUtils.format("'%s'(%s)", opName, type);
    }

    @Override
    public Consistency getConsistency()
    {
      return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i)
    {
      return false;
    }
  }
}
