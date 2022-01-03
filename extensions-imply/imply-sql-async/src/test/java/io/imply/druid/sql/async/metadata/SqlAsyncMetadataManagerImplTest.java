/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.sql.http.ResultFormat;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlAsyncMetadataManagerImplTest
{
  @Rule
  public final DerbyConnectorRule connectorRule = new DerbyConnectorRule();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private SqlAsyncMetadataManagerImpl metadataManager;

  @Before
  public void setup()
  {
    metadataManager = new SqlAsyncMetadataManagerImpl(
        jsonMapper,
        MetadataStorageConnectorConfig::new,
        new SqlAsyncMetadataStorageTableConfig(null, null),
        connectorRule.getConnector()
    );
    metadataManager.initialize();
  }

  @Test
  public void testAddAndGetQueryDetails() throws AsyncQueryAlreadyExistsException
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    assertQueryState(metadataManager, "id", queryDetails);
  }

  @Test
  public void testAddDuplicateId() throws AsyncQueryAlreadyExistsException
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    Assert.assertThrows(AsyncQueryAlreadyExistsException.class, () -> metadataManager.addNewQuery(queryDetails));
  }

  @Test
  public void testUpdateNonExistingQuery()
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("nonExisting", "identity", ResultFormat.CSV);
    Assert.assertThrows(
        AsyncQueryDoesNotExistException.class,
        () -> metadataManager.updateQueryDetails(queryDetails.toRunning())
    );
  }

  @Test
  public void testUpdateQueryFromGivenState() throws AsyncQueryDoesNotExistException, AsyncQueryAlreadyExistsException
  {
    SqlAsyncQueryDetails running = SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV).toRunning();
    SqlAsyncQueryDetails complete = running.toComplete(10L);
    metadataManager.addNewQuery(running);

    Assert.assertTrue(metadataManager.updateQueryDetails(complete));
    assertQueryState(metadataManager, "id1", complete);
  }

  /**
   * This test emulates the case where the query state has been updated by another thread
   * before the main thread updates it. The second update should succeed because the state is 'running'
   * when the main thread tries to update it.
   */
  @Test
  public void testUpdateQueryFromAnEarlierStateSucceed()
      throws AsyncQueryAlreadyExistsException, AsyncQueryDoesNotExistException
  {
    SqlAsyncQueryDetails initialized = SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV);
    SqlAsyncQueryDetails running = initialized.toRunning();
    SqlAsyncQueryDetails complete = running.toComplete(10L);
    metadataManager.addNewQuery(running);

    Assert.assertTrue(metadataManager.updateQueryDetails(complete));
    assertQueryState(metadataManager, "id1", complete);
  }

  /**
   * This test emulates the case where the query state has been updated by another thread
   * before the main thread updates it. The second update should fail because the state is 'error'
   * when the main thread tries to update it.
   */
  @Test
  public void testUpdateQueryFromAnEarlierStateFail()
      throws AsyncQueryAlreadyExistsException, AsyncQueryDoesNotExistException
  {
    SqlAsyncQueryDetails initialized = SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV);
    SqlAsyncQueryDetails error = initialized.toError(null);
    SqlAsyncQueryDetails complete = initialized.toComplete(10L);
    metadataManager.addNewQuery(error);

    Assert.assertFalse(metadataManager.updateQueryDetails(complete));
    assertQueryState(metadataManager, "id1", error);
  }

  @Test
  public void testRemoveNonExistingQuery()
  {
    Assert.assertFalse(
        metadataManager.removeQueryDetails(SqlAsyncQueryDetails.createNew("nonExisting", "identity", ResultFormat.CSV))
    );
  }

  @Test
  public void testRemoveExistingQuery() throws AsyncQueryAlreadyExistsException
  {
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    Assert.assertTrue(metadataManager.removeQueryDetails(queryDetails));
    Assert.assertFalse(metadataManager.getQueryDetails("id").isPresent());
  }

  @Test
  public void testGetQueryDetailsAndMetadata() throws AsyncQueryAlreadyExistsException
  {
    DateTime now = DateTimes.nowUtc();
    SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew("id", "identity", ResultFormat.CSV);
    metadataManager.addNewQuery(queryDetails);
    SqlAsyncQueryDetailsAndMetadata detailsAndMetadata = metadataManager
        .getQueryDetailsAndMetadata(queryDetails.getAsyncResultId())
        .orElseThrow(() -> new AssertionError("should exist"));
    Assert.assertEquals(queryDetails, detailsAndMetadata.getSqlAsyncQueryDetails());
    Assert.assertNotNull(detailsAndMetadata.getMetadata());
    Assert.assertTrue(detailsAndMetadata.getMetadata().getLastUpdatedTime() >= now.getMillis());
  }

  @Test
  public void testGetAllAsyncResultIds() throws AsyncQueryAlreadyExistsException
  {
    List<SqlAsyncQueryDetails> queries = ImmutableList.of(
        SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV),
        SqlAsyncQueryDetails.createNew("id2", "identity", ResultFormat.CSV),
        SqlAsyncQueryDetails.createNew("id3", "identity", ResultFormat.CSV)
    );
    for (SqlAsyncQueryDetails queryDetails : queries) {
      metadataManager.addNewQuery(queryDetails);
    }

    Assert.assertEquals(
        queries.stream().map(SqlAsyncQueryDetails::getAsyncResultId).collect(Collectors.toSet()),
        new HashSet<>(metadataManager.getAllAsyncResultIds())
    );
  }

  @Test
  public void testTotalCompleteQueryResultsSize() throws AsyncQueryAlreadyExistsException
  {
    List<SqlAsyncQueryDetails> queries = ImmutableList.of(
        SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV).toComplete(10L),
        SqlAsyncQueryDetails.createNew("id2", "identity", ResultFormat.CSV).toComplete(20L),
        SqlAsyncQueryDetails.createNew("id3", "identity", ResultFormat.CSV).toComplete(30L)
    );
    for (SqlAsyncQueryDetails queryDetails : queries) {
      metadataManager.addNewQuery(queryDetails);
    }
    Assert.assertEquals(60L, metadataManager.totalCompleteQueryResultsSize());
  }

  @Test
  public void testTotalCompleteQueryResultsSizeWithIdFilter() throws AsyncQueryAlreadyExistsException
  {
    List<SqlAsyncQueryDetails> queries = ImmutableList.of(
        SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV).toComplete(10L),
        SqlAsyncQueryDetails.createNew("id2", "identity", ResultFormat.CSV).toComplete(20L),
        SqlAsyncQueryDetails.createNew("id3", "identity", ResultFormat.CSV).toComplete(30L)
    );
    for (SqlAsyncQueryDetails queryDetails : queries) {
      metadataManager.addNewQuery(queryDetails);
    }
    Assert.assertEquals(40L, metadataManager.totalCompleteQueryResultsSize(ImmutableList.of("id1", "id3")));
  }


  @Test
  public void testTouchAsyncQuerySuccessfullyUpdatesQueryMetadata()
      throws AsyncQueryAlreadyExistsException, InterruptedException, AsyncQueryDoesNotExistException
  {
    SqlAsyncQueryDetails running = SqlAsyncQueryDetails.createNew("id1", "identity", ResultFormat.CSV).toRunning();
    metadataManager.addNewQuery(running);
    Optional<SqlAsyncQueryDetailsAndMetadata> detailsOpt = metadataManager.getQueryDetailsAndMetadata(running.getAsyncResultId());
    long lastUpateTime = detailsOpt.get().getMetadata().getLastUpdatedTime();
    Thread.sleep(10L);
    metadataManager.touchQueryLastUpdateTime(running.getAsyncResultId());
    Optional<SqlAsyncQueryDetailsAndMetadata> newDetailsOpt = metadataManager.getQueryDetailsAndMetadata(running.getAsyncResultId());
    long newLastUpateTime = newDetailsOpt.get().getMetadata().getLastUpdatedTime();
    Assert.assertNotEquals(lastUpateTime, newLastUpateTime);
  }

  static void assertQueryState(
      SqlAsyncMetadataManager metadataManager,
      String asyncResultId,
      SqlAsyncQueryDetails expected
  )
  {
    Assert.assertEquals(
        expected,
        metadataManager.getQueryDetails(asyncResultId).orElseThrow(() -> new AssertionError("should exist"))
    );
  }
}
