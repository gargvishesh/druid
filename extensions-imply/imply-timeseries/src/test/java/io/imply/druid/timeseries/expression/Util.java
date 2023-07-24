/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.expression;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.aggregation.BaseTimeSeriesAggregatorFactory;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Util
{
  @SuppressWarnings("ConstantConditions")
  public static void expectSimpleTimeseries(ExprEval<?> result, SimpleTimeSeries expectedDeltaTimeSeries)
  {
    Assert.assertEquals(expectedDeltaTimeSeries, ((SimpleTimeSeriesContainer) result.value()).computeSimple());
  }

  public static Expr.ObjectBinding makeBinding(String field, SimpleTimeSeries ts)
  {
    return makeBinding(field, new AtomicReference<>(SimpleTimeSeriesContainer.createFromInstance(ts)));
  }

  @SuppressWarnings("ConstantConditions")
  public static Expr.ObjectBinding makeBinding(String field, AtomicReference<SimpleTimeSeriesContainer> ref)
  {
    return InputBindings.forInputSuppliers(
        ImmutableMap.of(
            field,
            InputBindings.inputSupplier(
                ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE),
                ref::get
            )
        )
    );
  }

  @SuppressWarnings("ConstantConditions")
  public static Expr.ObjectBinding makeBindings(Map<String, Object> bindings)
  {
    ImmutableMap.Builder<String, InputBindings.InputSupplier<?>> suppliers = ImmutableMap.builder();
    for (Map.Entry<String, Object> binding : bindings.entrySet()) {
      suppliers.put(
          binding.getKey(),
          InputBindings.inputSupplier(
          ExpressionType.fromColumnType(BaseTimeSeriesAggregatorFactory.TYPE),
          binding::getValue
          )
      );
    }
    return InputBindings.forInputSuppliers(suppliers.build());
  }
}
