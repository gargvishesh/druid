/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import io.imply.druid.sql.async.query.SqlAsyncQueryPool;
import io.imply.druid.sql.async.query.SqlAsyncQueryStatsMonitor;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SqlAsyncQueryStatsMonitorTest
{
  @Mock
  private SqlAsyncQueryPool mockSqlAsyncQueryPool;

  @Mock
  private AsyncQueryConfig mockAsyncQueryLimitsConfig;

  @Mock
  private ServiceEmitter mockServiceEmitter;

  @Test
  public void testRun()
  {
    SqlAsyncQueryPool.BestEffortStatsSnapshot sqlAsyncQueryPoolStats = new SqlAsyncQueryPool.BestEffortStatsSnapshot(2, 3);
    Mockito.when(mockSqlAsyncQueryPool.getBestEffortStatsSnapshot()).thenReturn(sqlAsyncQueryPoolStats);

    SqlAsyncQueryStatsMonitor sqlAsyncQueryStatsMonitor = new SqlAsyncQueryStatsMonitor(
        "brokerId123",
        mockSqlAsyncQueryPool,
        mockAsyncQueryLimitsConfig
    );
    sqlAsyncQueryStatsMonitor.doMonitor(mockServiceEmitter);
    Mockito.verify(mockServiceEmitter, Mockito.times(4)).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito.verifyNoMoreInteractions(mockServiceEmitter);
  }
}
