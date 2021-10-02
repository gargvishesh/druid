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
  private SqlAsyncMetadataManager mockSqlAsyncMetadataManager;

  @Mock
  private ServiceEmitter mockServiceEmitter;

  @Test
  public void testRun()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics(mockSqlAsyncMetadataManager);
    CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_SUCCEED_COUNT_STAT_KEY, 1);
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_FAILED_COUNT_STAT_KEY, 2);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY, 3);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_FAILED_COUNT_STAT_KEY, 4);
    stats.addToGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY, 5);
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_SUCCEED_SIZE_STAT_KEY, 6);
    stats.addToGlobalStat(KillAsyncQueryResultWithoutMetadata.RESULT_REMOVED_FAILED_SIZE_STAT_KEY, 7);
    stats.addToGlobalStat(UpdateStaleQueryState.STALE_QUERIES_MARKED_UNDETERMINED_COUNT, 8);
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                                                              .withCoordinatorStats(stats)
                                                                              .withEmitter(mockServiceEmitter)
                                                                              .build();
    emitAsyncStatsAndMetrics.run(params);
    Mockito.verify(mockServiceEmitter, Mockito.times(10)).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito.verifyNoMoreInteractions(mockServiceEmitter);
  }

  @Test
  public void testRunWithSomeDutyMetricMissing()
  {
    EmitAsyncStatsAndMetrics emitAsyncStatsAndMetrics = new EmitAsyncStatsAndMetrics(mockSqlAsyncMetadataManager);
    CoordinatorStats stats = new CoordinatorStats();
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                                                              .withCoordinatorStats(stats)
                                                                              .withEmitter(mockServiceEmitter)
                                                                              .build();
    emitAsyncStatsAndMetrics.run(params);
    Mockito.verify(mockServiceEmitter, Mockito.times(10)).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito.verifyNoMoreInteractions(mockServiceEmitter);
  }
}
