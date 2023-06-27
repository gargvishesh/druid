/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;

public class TimeseriesExprUtil
{
  public static Object expectLiteral(Expr arg, String fnName, int index)
  {
    if (arg.isLiteral()) {
      return arg.getLiteralValue();
    } else {
      throw new IAE(
          "Function [%s] expects argument at position [%s] to be a literal, got [%s]",
          fnName,
          index,
          arg.stringify()
      );
    }
  }
}
