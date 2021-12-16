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
import io.imply.druid.sql.async.coordinator.SqlAsyncCleanupModule;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;
import io.imply.druid.sql.async.query.SqlAsyncQueryMetadata;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.sql.http.ResultFormat;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class KillAsyncQueryMetadataTest
{
  private static final String IDENTITY = "identity-123";
  private static final ResultFormat RESULT_FORMAT = ResultFormat.ARRAY;

  @Mock
  private SqlAsyncMetadataManager mockSqlAsyncMetadataManager;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private KillAsyncQueryMetadata killAsyncQueryMetadata;

  @Test
  public void testConstructorFailIfRetainDurationNull()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        StringUtils.format(
            "Coordinator async result cleanup duty timeToRetain [%s] must be >= 0",
            SqlAsyncCleanupModule.CLEANUP_TIME_TO_RETAIN_CONFIG_KEY
        )
    );
    killAsyncQueryMetadata = new KillAsyncQueryMetadata(null, mockSqlAsyncMetadataManager);
  }

  @Test
  public void testConstructorFailIfRetainDurationInvalid()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        StringUtils.format(
            "Coordinator async result cleanup duty timeToRetain [%s] must be >= 0",
            SqlAsyncCleanupModule.CLEANUP_TIME_TO_RETAIN_CONFIG_KEY
        )
    );
    killAsyncQueryMetadata = new KillAsyncQueryMetadata(new Duration("PT-1S"), mockSqlAsyncMetadataManager);
  }

  @Test
  public void testConstructorSuccess()
  {
    killAsyncQueryMetadata = new KillAsyncQueryMetadata(new Duration("PT1S"), mockSqlAsyncMetadataManager);
    Assert.assertNotNull(killAsyncQueryMetadata);
  }

  @Test
  public void testIsQueryEligibleForCleanupNotEligibleState()
  {
    SqlAsyncQueryDetails sqlAsyncQueryDetails = SqlAsyncQueryDetails.createNew(
        "asyncResultId1",
        IDENTITY,
        RESULT_FORMAT
    );
    // Not eligible for cleanup since state is INITIALIZED
    Assert.assertFalse(KillAsyncQueryMetadata.isQueryEligibleForCleanup(sqlAsyncQueryDetails, 0, 0));
    sqlAsyncQueryDetails = sqlAsyncQueryDetails.toRunning();
    // Not eligible for cleanup since state is RUNNING
    Assert.assertFalse(KillAsyncQueryMetadata.isQueryEligibleForCleanup(sqlAsyncQueryDetails, 0, 0));
  }

  @Test
  public void testIsQueryEligibleForCleanupEligibleState()
  {
    SqlAsyncQueryDetails sqlAsyncQueryDetails = SqlAsyncQueryDetails.createNew(
        "asyncResultId1",
        IDENTITY,
        RESULT_FORMAT
    );
    sqlAsyncQueryDetails = sqlAsyncQueryDetails.toComplete(100);
    // Eligible for cleanup since state is COMPLETED
    Assert.assertTrue(KillAsyncQueryMetadata.isQueryEligibleForCleanup(sqlAsyncQueryDetails, 0, 0));
    sqlAsyncQueryDetails = sqlAsyncQueryDetails.toError(new RuntimeException());
    // Eligible for cleanup since state is ERROR
    Assert.assertTrue(KillAsyncQueryMetadata.isQueryEligibleForCleanup(sqlAsyncQueryDetails, 0, 0));
  }

  @Test
  public void testIsQueryEligibleForCleanupForRetainTime()
  {
    SqlAsyncQueryDetails sqlAsyncQueryDetails = SqlAsyncQueryDetails.createNew(
        "asyncResultId1",
        IDENTITY,
        RESULT_FORMAT
    );
    sqlAsyncQueryDetails = sqlAsyncQueryDetails.toComplete(100);
    // Eligible for cleanup since updatedTime is older than current time - retianDuration
    Assert.assertTrue(KillAsyncQueryMetadata.isQueryEligibleForCleanup(sqlAsyncQueryDetails, 0, 100000));
    // Not eligible for cleanup since updatedTime is newer than current time - retianDuration
    Assert.assertFalse(KillAsyncQueryMetadata.isQueryEligibleForCleanup(
        sqlAsyncQueryDetails,
        System.currentTimeMillis(),
        100000
    ));
  }

  @Test
  public void testRun()
  {
    // query1 is eligible for cleanup
    String query1 = "asyncResultId1";
    SqlAsyncQueryDetails sqlAsyncQueryDetail1 = SqlAsyncQueryDetails.createNew(query1, IDENTITY, RESULT_FORMAT)
                                                                    .toComplete(100);
    SqlAsyncQueryMetadata metadata1 = new SqlAsyncQueryMetadata(0);
    SqlAsyncQueryDetailsAndMetadata sqlAsyncQueryDetailAndStat1 = new SqlAsyncQueryDetailsAndMetadata(
        sqlAsyncQueryDetail1,
        metadata1
    );

    // query2 is not eligible for cleanup due to running state
    String query2 = "asyncResultId2";
    SqlAsyncQueryDetails sqlAsyncQueryDetail2 = SqlAsyncQueryDetails.createNew(query2, IDENTITY, RESULT_FORMAT).toRunning();
    SqlAsyncQueryMetadata metadata2 = new SqlAsyncQueryMetadata(0);
    SqlAsyncQueryDetailsAndMetadata sqlAsyncQueryDetailAndStat2 = new SqlAsyncQueryDetailsAndMetadata(
        sqlAsyncQueryDetail2,
        metadata2
    );

    // query3 is unexpected error where there is no metadata / does not exist
    String query3 = "asyncResultId3";

    // query4 is not eligible for cleanup due to within retian time
    String query4 = "asyncResultId4";
    SqlAsyncQueryDetails sqlAsyncQueryDetail4 = SqlAsyncQueryDetails.createNew(query4, IDENTITY, RESULT_FORMAT)
                                                                    .toComplete(100);
    SqlAsyncQueryMetadata metadata4 = new SqlAsyncQueryMetadata(System.currentTimeMillis());
    SqlAsyncQueryDetailsAndMetadata sqlAsyncQueryDetailAndStat4 = new SqlAsyncQueryDetailsAndMetadata(
        sqlAsyncQueryDetail4,
        metadata4
    );

    Collection<String> asyncResultIds = ImmutableList.of(query1, query2, query3, query4);
    Mockito.when(mockSqlAsyncMetadataManager.getAllAsyncResultIds()).thenReturn(asyncResultIds);
    Mockito.when(mockSqlAsyncMetadataManager.getQueryDetailsAndMetadata(ArgumentMatchers.eq(query1)))
           .thenReturn(Optional.of(sqlAsyncQueryDetailAndStat1));
    Mockito.when(mockSqlAsyncMetadataManager.getQueryDetailsAndMetadata(ArgumentMatchers.eq(query2)))
           .thenReturn(Optional.of(sqlAsyncQueryDetailAndStat2));
    Mockito.when(mockSqlAsyncMetadataManager.getQueryDetailsAndMetadata(ArgumentMatchers.eq(query3)))
           .thenReturn(Optional.empty());
    Mockito.when(mockSqlAsyncMetadataManager.getQueryDetailsAndMetadata(ArgumentMatchers.eq(query4)))
           .thenReturn(Optional.of(sqlAsyncQueryDetailAndStat4));

    Mockito.when(mockSqlAsyncMetadataManager.removeQueryDetails(ArgumentMatchers.eq(sqlAsyncQueryDetail1)))
           .thenReturn(true);

    killAsyncQueryMetadata = new KillAsyncQueryMetadata(new Duration("PT60000S"), mockSqlAsyncMetadataManager);
    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers.newBuilder().build();
    killAsyncQueryMetadata.run(params);
    Assert.assertEquals(
        1,
        params.getCoordinatorStats().getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SUCCEED_COUNT_STAT_KEY)
    );
    Assert.assertEquals(
        1,
        params.getCoordinatorStats().getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_FAILED_COUNT_STAT_KEY)
    );
    Assert.assertEquals(
        2,
        params.getCoordinatorStats().getGlobalStat(KillAsyncQueryMetadata.METADATA_REMOVED_SKIPPED_COUNT_STAT_KEY)
    );

    Mockito.verify(mockSqlAsyncMetadataManager).removeQueryDetails(ArgumentMatchers.eq(sqlAsyncQueryDetail1));

    Mockito.verify(mockSqlAsyncMetadataManager).getAllAsyncResultIds();
    Mockito.verify(mockSqlAsyncMetadataManager, Mockito.times(4)).getQueryDetailsAndMetadata(ArgumentMatchers.any());

    Mockito.verifyNoMoreInteractions(mockSqlAsyncMetadataManager);
  }
}
