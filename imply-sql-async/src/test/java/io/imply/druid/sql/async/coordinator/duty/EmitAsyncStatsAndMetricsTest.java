/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EmitAsyncStatsAndMetricsTest
{
  @Mock
  private ServiceEmitter mockServiceEmitter;

  @Test
  public void testRun()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics();
    CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_SUCCEED_COUNT_STAT_KEY, 1);
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_FAILED_COUNT_STAT_KEY, 2);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY, 3);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_FAILED_COUNT_STAT_KEY, 4);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY, 5);
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                                                              .withCoordinatorStats(stats)
                                                                              .withEmitter(mockServiceEmitter)
                                                                              .build();
    emitAsyncStatsAndMetrics.run(params);
    Mockito.verify(mockServiceEmitter, Mockito.times(5)).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito.verifyNoMoreInteractions(mockServiceEmitter);
  }

  @Test
  public void testRunWithSomeDutyMetricMissing()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics();
    CoordinatorStats stats = new CoordinatorStats();
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                                                              .withCoordinatorStats(stats)
                                                                              .withEmitter(mockServiceEmitter)
                                                                              .build();
    emitAsyncStatsAndMetrics.run(params);
    Mockito.verify(mockServiceEmitter, Mockito.times(5)).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito.verifyNoMoreInteractions(mockServiceEmitter);
  }
}
