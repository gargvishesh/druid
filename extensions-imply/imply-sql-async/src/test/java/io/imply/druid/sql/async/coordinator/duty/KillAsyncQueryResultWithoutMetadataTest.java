/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import com.google.common.collect.ImmutableList;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.result.SqlAsyncResultManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillAsyncQueryResultWithoutMetadataTest
{
  @Mock
  private SqlAsyncResultManager mockSqlAsyncResultManager;

  @Mock
  private SqlAsyncMetadataManager mockSqlAsyncMetadataManager;

  private KillAsyncQueryResultWithoutMetadata killAsyncQueryResultWithoutMetadata;

  @Test
  public void testRun() throws Exception
  {
    // query1 has metadata
    String query1 = "asyncResultId1";

    // query2 does not have metadata
    String query2 = "asyncResultId2";
    long query2Size = 5L;

    // query3 fail to remove
    String query3 = "asyncResultId3";
    long query3Size = 10L;

    Mockito.when(mockSqlAsyncMetadataManager.getAllAsyncResultIds()).thenReturn(ImmutableList.of(query1));
    Mockito.when(mockSqlAsyncResultManager.getAllAsyncResultIds()).thenReturn(ImmutableList.of(query1, query2, query3));
    Mockito.when(mockSqlAsyncResultManager.deleteResults(ArgumentMatchers.eq(query2))).thenReturn(true);
    Mockito.when(mockSqlAsyncResultManager.deleteResults(ArgumentMatchers.eq(query3))).thenReturn(false);
    Mockito.when(mockSqlAsyncResultManager.getResultSize(ArgumentMatchers.eq(query2))).thenReturn(query2Size);
    Mockito.when(mockSqlAsyncResultManager.getResultSize(ArgumentMatchers.eq(query3))).thenReturn(query3Size);

    killAsyncQueryResultWithoutMetadata = new KillAsyncQueryResultWithoutMetadata(mockSqlAsyncResultManager, mockSqlAsyncMetadataManager);
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder(System.nanoTime()).build();
    killAsyncQueryResultWithoutMetadata.run(params);

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertEquals(1, stats.get(Stats.RESULT_CLEANUP_SUCCESS_COUNT));
    Assert.assertEquals(1, stats.get(Stats.RESULT_CLEANUP_FAILED_COUNT));
    Assert.assertEquals(query2Size, stats.get(Stats.RESULT_CLEANUP_SUCCESS_BYTES));
    Assert.assertEquals(query3Size, stats.get(Stats.RESULT_CLEANUP_FAILED_BYTES));
    Mockito.verify(mockSqlAsyncResultManager).getAllAsyncResultIds();
    Mockito.verify(mockSqlAsyncResultManager).deleteResults(ArgumentMatchers.eq(query2));
    Mockito.verify(mockSqlAsyncResultManager).deleteResults(ArgumentMatchers.eq(query3));
    Mockito.verify(mockSqlAsyncMetadataManager).getAllAsyncResultIds();
    Mockito.verify(mockSqlAsyncResultManager).getResultSize(ArgumentMatchers.eq(query2));
    Mockito.verify(mockSqlAsyncResultManager).getResultSize(ArgumentMatchers.eq(query3));

    Mockito.verifyNoMoreInteractions(mockSqlAsyncResultManager);
    Mockito.verifyNoMoreInteractions(mockSqlAsyncMetadataManager);

  }
}
