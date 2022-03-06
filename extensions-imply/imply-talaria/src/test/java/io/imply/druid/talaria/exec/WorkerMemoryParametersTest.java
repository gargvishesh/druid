/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.error.NotEnoughMemoryFault;
import io.imply.druid.talaria.indexing.error.TalariaException;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import org.junit.Assert;
import org.junit.Test;

public class WorkerMemoryParametersTest
{
  @Test
  public void test_oneWorkerInJvm_alone()
  {
    Assert.assertEquals(parameters(1, 45, 249_910_000), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 14, 166_160_000), compute(1_000_000_000, 1, 2, 1));
    Assert.assertEquals(parameters(4, 3, 99_160_000), compute(1_000_000_000, 1, 4, 1));
    Assert.assertEquals(parameters(3, 2, 54_493_333), compute(1_000_000_000, 1, 8, 1));
    Assert.assertEquals(parameters(1, 4, 28_218_823), compute(1_000_000_000, 1, 16, 1));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(1_000_000_000, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(1_000_000_000, 1, 32), e.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 45, 116_580_000), compute(1_000_000_000, 1, 1, 200));
    Assert.assertEquals(parameters(2, 14, 32_830_000), compute(1_000_000_000, 1, 2, 200));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(1_000_000_000, 1, 4, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 124), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 149, 669_330_000), compute(8_000_000_000L, 4, 1, 200));
    Assert.assertEquals(parameters(2, 61, 535_330_000), compute(8_000_000_000L, 4, 2, 200));
    Assert.assertEquals(parameters(4, 22, 367_830_000), compute(8_000_000_000L, 4, 4, 200));
    Assert.assertEquals(parameters(4, 14, 200_330_000), compute(8_000_000_000L, 4, 8, 200));
    Assert.assertEquals(parameters(4, 8, 66_330_000), compute(8_000_000_000L, 4, 16, 200));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(8_000_000_000L, 4, 32, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 140), e.getFault());

    // Make sure 140 actually works. (Verify the error message above.)
    Assert.assertEquals(parameters(4, 4, 17_196_666), compute(8_000_000_000L, 4, 32, 140));
  }

  private static WorkerMemoryParameters parameters(
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final long appenderatorMemory
  )
  {
    return new WorkerMemoryParameters(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        appenderatorMemory,
        (long) (appenderatorMemory * WorkerMemoryParameters.BROADCAST_JOIN_MEMORY_FRACTION)
    );
  }

  private static WorkerMemoryParameters compute(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers
  )
  {
    return WorkerMemoryParameters.compute(
        maxMemoryInJvm,
        numWorkersInJvm,
        numProcessingThreadsInJvm,
        numInputWorkers
    );
  }
}
