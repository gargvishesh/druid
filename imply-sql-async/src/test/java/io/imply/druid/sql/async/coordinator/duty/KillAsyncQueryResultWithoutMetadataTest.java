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
import io.imply.druid.sql.async.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.SqlAsyncResultManager;
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

  private KillAsyncQueryResultWithoutMetadata KillAsyncQueryResultWithoutMetadata;

  @Test
  public void testRun() throws Exception
  {
    // query1 has metadata
    String query1 = "asyncResultId1";

    // query2 does not have metadata
    String query2 = "asyncResultId2";

    Mockito.when(mockSqlAsyncMetadataManager.getAllAsyncResultIds()).thenReturn(ImmutableList.of(query1));
    Mockito.when(mockSqlAsyncResultManager.getAllResults()).thenReturn(ImmutableList.of(query1, query2));
    Mockito.when(mockSqlAsyncResultManager.deleteResults(ArgumentMatchers.eq(query2))).thenReturn(true);

    KillAsyncQueryResultWithoutMetadata = new KillAsyncQueryResultWithoutMetadata(mockSqlAsyncResultManager, mockSqlAsyncMetadataManager);
    KillAsyncQueryResultWithoutMetadata.run(null);

    Mockito.verify(mockSqlAsyncResultManager).getAllResults();
    Mockito.verify(mockSqlAsyncResultManager).deleteResults(ArgumentMatchers.eq(query2));
    Mockito.verify(mockSqlAsyncMetadataManager).getAllAsyncResultIds();
    Mockito.verifyNoMoreInteractions(mockSqlAsyncResultManager);
    Mockito.verifyNoMoreInteractions(mockSqlAsyncMetadataManager);
  }
}
