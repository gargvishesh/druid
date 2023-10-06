/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.column.ColumnType;

import java.util.Objects;

public class TimeseriesToJSONExprMacro extends UnaryTimeseriesExprMacro
{
  public static final String NAME = "timeseries_to_json";
  public static final ColumnType TYPE = ColumnType.ofComplex("imply-ts-json");
  private static final ExpressionType OUTPUT_TYPE =
      Objects.requireNonNull(ExpressionType.fromColumnType(TYPE), "type is null");

  @Override
  public ExpressionType getType()
  {
    return OUTPUT_TYPE;
  }

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public ExprEval compute(SimpleTimeSeriesContainer simpleTimeSeriesContainer)
  {
    if (simpleTimeSeriesContainer.isNull()) {
      return ExprEval.ofComplex(
          OUTPUT_TYPE,
          null
      );
    }
    return ExprEval.ofComplex(
        OUTPUT_TYPE,
        simpleTimeSeriesContainer.computeSimple()
    );
  }
}
