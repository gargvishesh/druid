/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class EmitAsyncStatsAndMetricsTest
{
  @Mock
  private SqlAsyncMetadataManager mockSqlAsyncMetadataManager;

  @Test
  public void testRun()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics(mockSqlAsyncMetadataManager);
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder(System.nanoTime()).build();

    Mockito.when(mockSqlAsyncMetadataManager.getAllAsyncResultIds())
           .thenReturn(Arrays.asList("resultId1", "resultId2"));
    params = emitAsyncStatsAndMetrics.run(params);
    Assert.assertEquals(2L, params.getCoordinatorStats().get(Stats.TRACKED_RESULTS));
  }

  @Test
  public void testRunWithSomeDutyMetricMissing()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics(mockSqlAsyncMetadataManager);
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder(System.nanoTime()).build();

    Mockito.when(mockSqlAsyncMetadataManager.getAllAsyncResultIds())
           .thenReturn(Arrays.asList("resultId1", "resultId2"));
    params = emitAsyncStatsAndMetrics.run(params);
    Assert.assertEquals(2L, params.getCoordinatorStats().get(Stats.TRACKED_RESULTS));
  }
}
