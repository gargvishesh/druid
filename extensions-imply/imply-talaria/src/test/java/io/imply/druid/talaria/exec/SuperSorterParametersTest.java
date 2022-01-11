/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import org.junit.Assert;
import org.junit.Test;

public class SuperSorterParametersTest
{
  @Test
  public void test_oneWorker_capped()
  {
    Assert.assertEquals(parameters(1, 73), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 35), compute(1_000_000_000, 1, 2, 2));
    Assert.assertEquals(parameters(2, 34), compute(1_000_000_000, 1, 4, 2));
    Assert.assertEquals(parameters(2, 32), compute(1_000_000_000, 1, 8, 2));
    Assert.assertEquals(parameters(2, 28), compute(1_000_000_000, 1, 16, 2));
    Assert.assertEquals(parameters(2, 20), compute(1_000_000_000, 1, 32, 2));
    Assert.assertEquals(parameters(2, 4), compute(1_000_000_000, 1, 64, 2));

    Assert.assertThrows(
        "Not enough memory for frames",
        IllegalStateException.class,
        () -> compute(1_000_000_000, 1, 128, 2)
    );
  }

  @Test
  public void test_oneWorker_uncapped()
  {
    Assert.assertEquals(parameters(1, 73), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 35), compute(1_000_000_000, 1, 2, 2));
    Assert.assertEquals(parameters(4, 16), compute(1_000_000_000, 1, 4, 4));
    Assert.assertEquals(parameters(8, 7), compute(1_000_000_000, 1, 8, 8));
    Assert.assertEquals(parameters(16, 2), compute(1_000_000_000, 1, 16, 16));
    Assert.assertEquals(parameters(14, 2), compute(1_000_000_000, 1, 32, 32));
    Assert.assertEquals(parameters(3, 2), compute(1_000_000_000, 1, 64, 64));

    Assert.assertThrows(
        "Not enough memory for frames",
        IllegalStateException.class,
        () -> compute(1_000_000_000, 1, 128, 128)
    );
  }

  @Test
  public void test_fourWorkers_uncapped()
  {
    Assert.assertEquals(parameters(1, 16), compute(1_000_000_000, 4, 1, 1));
    Assert.assertEquals(parameters(2, 7), compute(1_000_000_000, 4, 2, 2));
    Assert.assertEquals(parameters(4, 2), compute(1_000_000_000, 4, 4, 4));
    Assert.assertEquals(parameters(3, 2), compute(1_000_000_000, 4, 8, 8));

    Assert.assertThrows(
        "Not enough memory for frames",
        IllegalStateException.class,
        () -> compute(1_000_000_000, 4, 16, 16)
    );
  }

  private static WorkerImpl.SuperSorterParameters parameters(int numProcessors, int channelsPerProcessor)
  {
    return new WorkerImpl.SuperSorterParameters(numProcessors, channelsPerProcessor);
  }

  private static WorkerImpl.SuperSorterParameters compute(
      final long maxMemory,
      final int numWorkersInJvm,
      final int maxProcessors,
      final int maxSuperSorterProcessors
  )
  {
    return WorkerImpl.SuperSorterParameters.compute(
        maxMemory,
        numWorkersInJvm,
        maxProcessors,
        maxSuperSorterProcessors
    );
  }
}
