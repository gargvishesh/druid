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
    Assert.assertEquals(parameters(1, 45, 373_000_000), compute(1_000_000_000, 1, 1, 1));
    Assert.assertEquals(parameters(2, 14, 248_000_000), compute(1_000_000_000, 1, 2, 1));
    Assert.assertEquals(parameters(4, 3, 148_000_000), compute(1_000_000_000, 1, 4, 1));
    Assert.assertEquals(parameters(3, 2, 81_333_333), compute(1_000_000_000, 1, 8, 1));
    Assert.assertEquals(parameters(1, 4, 42_117_647), compute(1_000_000_000, 1, 16, 1));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(1_000_000_000, 1, 32, 1)
    );

    Assert.assertEquals(new NotEnoughMemoryFault(1_000_000_000, 1, 32), e.getFault());
  }

  @Test
  public void test_oneWorkerInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 45, 174_000_000), compute(1_000_000_000, 1, 1, 200));
    Assert.assertEquals(parameters(2, 14, 49_000_000), compute(1_000_000_000, 1, 2, 200));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(1_000_000_000, 1, 4, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 124), e.getFault());
  }

  @Test
  public void test_fourWorkersInJvm_twoHundredWorkersInCluster()
  {
    Assert.assertEquals(parameters(1, 149, 999_000_000), compute(8_000_000_000L, 4, 1, 200));
    Assert.assertEquals(parameters(2, 61, 799_000_000), compute(8_000_000_000L, 4, 2, 200));
    Assert.assertEquals(parameters(4, 22, 549_000_000), compute(8_000_000_000L, 4, 4, 200));
    Assert.assertEquals(parameters(4, 14, 299_000_000), compute(8_000_000_000L, 4, 8, 200));
    Assert.assertEquals(parameters(4, 8, 99_000_000), compute(8_000_000_000L, 4, 16, 200));

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> compute(8_000_000_000L, 4, 32, 200)
    );

    Assert.assertEquals(new TooManyWorkersFault(200, 140), e.getFault());

    // Make sure 140 actually works. (Verify the error message above.)
    Assert.assertEquals(parameters(4, 4, 25_666_666), compute(8_000_000_000L, 4, 32, 140));
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
