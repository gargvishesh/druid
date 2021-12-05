/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.sql;

import io.imply.druid.timeseries.interpolation.Interpolator;
import io.imply.druid.timeseries.postaggregators.TimeWeightedAveragePostAggregator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

public class TimeWeightedAverageOperatorConversion extends DirectOperatorConversion
{
  private static final String FUNCTION_NAME = "time_weighted_average";
  private static final SqlOperator FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
      .operandTypeChecker(OperandTypes.sequence("'" + FUNCTION_NAME + "'(timeseries, interpolator, bucketPeriod)",
                                                OperandTypes.ANY,
                                                OperandTypes.LITERAL,
                                                OperandTypes.LITERAL))
      .returnTypeInference(ReturnTypes.explicit(SqlTypeName.OTHER))
      .build();

  public TimeWeightedAverageOperatorConversion()
  {
    super(FUNCTION, FUNCTION_NAME);
  }

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    List<RexNode> operands = ((RexCall) rexNode).getOperands();
    PostAggregator timeseries = OperatorConversions.toPostAggregator(
        plannerContext,
        rowSignature,
        operands.get(0),
        postAggregatorVisitor
    );

    if (timeseries == null) {
      return null;
    }

    // get the interpolator
    String interpolator = RexLiteral.stringValue(operands.get(1));

    // get the time bucket millis for intervals
    Long timeBucketMillis = new Period(RexLiteral.stringValue(operands.get(2))).toStandardDuration().getMillis();

    return new TimeWeightedAveragePostAggregator(
        postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
        timeseries,
        Interpolator.valueOf(interpolator.toUpperCase(Locale.ROOT)),
        timeBucketMillis
    );
  }
}
