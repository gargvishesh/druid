/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches;

import com.google.common.collect.ImmutableMap;
import com.google.zetasketch.HyperLogLogPlusPlus;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;
import org.apache.druid.query.expression.MacroTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

public class GcpHllToApacheHllExpressionTest extends MacroTestBase
{

  public GcpHllToApacheHllExpressionTest()
  {
    super(new GcpHllToApacheHllExpression());
  }

  @Test
  public void testConvertGBQHLLToApacheHLLSmallNumbers()
  {
    runTestFromTo(0, 4_000, 1234L);
  }

  @Test
  public void testConvertGBQHLLToApacheHLLSomeBigNumbers()
  {
    ArrayList<Integer> ints = new ArrayList<>();
    ints.add(42_861_596);
    ints.add(70_804_772);
    ints.add(30_456_813);
    ints.add(2_930_241);
    ints.add(804_772);
    ints.add(8_638_601);
    ints.add(6_690_176);
    ints.add(-1);

    runTest(0, ints::get, 4567L, 12, 12);
    runTest(0, ints::get, 4567L, 15, 15);
    runTest(0, ints::get, 4567L, 15, 20);
    runTest(0, ints::get, 4567L, 20, 20);
  }

  private void runTestFromTo(int from, int to, long seed)
  {
    runTest(from, index -> index >= to ? -1 : index, seed, 12, 12);
    runTest(from, index -> index >= to ? -1 : index, seed, 15, 15);
    runTest(from, index -> index >= to ? -1 : index, seed, 15, 20);
    runTest(from, index -> index >= to ? -1 : index, seed, 20, 20);
  }

  private void runTest(
      int startFromIndex,
      HllTestNumberSupplier supplier,
      long seed,
      int densePrecision,
      int sparsePrecision
  )
  {
    Random rand = new Random(seed);

    for (int index = 0, numItems = supplier.get(index); numItems >= 0; ++index, numItems = supplier.get(index)) {
      if (index < startFromIndex) {
        // Move the rand forward.
        for (int j = 0; j < numItems; ++j) {
          rand.nextInt();
        }
        continue;
      }

      String msg = StringUtils.format(
          "index[%,d], numItems[%,d], densePrecision[%s], sparsePrecision[%s]",
          index,
          numItems,
          densePrecision,
          sparsePrecision
      );
      try {
        HyperLogLogPlusPlus<Integer> hllpp = new HyperLogLogPlusPlus.Builder()
            .normalPrecision(densePrecision)
            .sparsePrecision(sparsePrecision)
            .buildForIntegers();
        for (int j = 0; j < numItems; j++) {
          hllpp.add(rand.nextInt());
        }

        final byte[] asBytes = hllpp.serializeToByteArray();
        final double expectedEstimate = hllpp.result();

        final ExprEval<?> eval = eval("gbq_hll_to_ds_hll(a)", InputBindings.forInputSuppliers(
            ImmutableMap.of("a", InputBindings.inputSupplier(ExpressionType.STRING, () -> asBytes))
        ));

        if (numItems == 0) {
          Assert.assertNull("0", eval.value());
        } else {
          Assert.assertTrue(msg, eval.value() instanceof HllSketchHolder);

          HllSketchHolder holder = (HllSketchHolder) eval.value();
          HllSketch sketch = holder.getSketch();

          Assert.assertEquals(msg, densePrecision, sketch.getLgConfigK());
          Assert.assertEquals(msg, densePrecision, hllpp.getNormalPrecision());
          Assert.assertEquals(msg, expectedEstimate, holder.getEstimate(), Math.ceil(expectedEstimate * 0.05));
        }
      }
      catch (Exception e) {
        throw new RE(e, "Failed on run: %s", msg);
      }
    }
  }

  public interface HllTestNumberSupplier
  {
    int get(int index);
  }
}
