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
import com.google.inject.Inject;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.exception.AsyncQueryAlreadyExistsException;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails.State;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsAndMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CuratorSqlAsyncMetadataManager implements SqlAsyncMetadataManager
{
  private static final String PATH = "imply-async-query";

  private final CuratorFramework curator;
  private final ZkPathsConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public CuratorSqlAsyncMetadataManager(
      CuratorFramework curator,
      ZkPathsConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.curator = curator;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void addNewQuery(final SqlAsyncQueryDetails queryDetails) throws IOException, AsyncQueryAlreadyExistsException
  {
    final String path = makeAsyncQueryStatePath(queryDetails.getAsyncResultId());
    final byte[] payload = jsonMapper.writeValueAsBytes(queryDetails);

    try {
      curator.create().creatingParentsIfNeeded().forPath(path, payload);
    }
    catch (KeeperException.NodeExistsException e) {
      throw new AsyncQueryAlreadyExistsException(queryDetails.getAsyncResultId());
    }
    catch (Exception e) {
      throw new IOE(e, "Error while creating path at [%s]", path);
    }
  }

  @Override
  public void updateQueryDetails(final SqlAsyncQueryDetails queryDetails)
      throws IOException, AsyncQueryDoesNotExistException
  {
    final String path = makeAsyncQueryStatePath(queryDetails.getAsyncResultId());
    final byte[] payload = jsonMapper.writeValueAsBytes(queryDetails);

    try {
      curator.setData().forPath(path, payload);
    }
    catch (KeeperException.NoNodeException e) {
      throw new AsyncQueryDoesNotExistException(queryDetails.getAsyncResultId());
    }
    catch (Exception e) {
      // oops, something bad happened. but don't worry. coordinator will clean up stale query states.
      // see UpdateStaleQueryState.
      throw new IOE(e, "Error while updating path at [%s]", path);
    }
  }

  @Override
  public boolean removeQueryDetails(final SqlAsyncQueryDetails queryDetails) throws IOException
  {
    final String path = makeAsyncQueryStatePath(queryDetails.getAsyncResultId());

    try {
      curator.delete().forPath(path);
      return true;
    }
    catch (KeeperException.NoNodeException e) {
      return false;
    }
    catch (Exception e) {
      throw new IOE(e, "Error while deleting path at [%s]", path);
    }
  }

  @Override
  public Optional<SqlAsyncQueryDetails> getQueryDetails(final String asyncResultId) throws IOException
  {
    Optional<SqlAsyncQueryDetailsAndMetadata> sqlAsyncQueryDetailsAndStatOptional = getQueryDetailsAndMetadataHelper(
        asyncResultId,
        null
    );
    return sqlAsyncQueryDetailsAndStatOptional.map(SqlAsyncQueryDetailsAndMetadata::getSqlAsyncQueryDetails);
  }

  @Override
  public Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadata(final String asyncResultId)
      throws IOException
  {
    Stat stat = new Stat();
    return getQueryDetailsAndMetadataHelper(asyncResultId, stat);
  }

  private Optional<SqlAsyncQueryDetailsAndMetadata> getQueryDetailsAndMetadataHelper(
      final String asyncResultId,
      @Nullable final Stat stat
  ) throws IOException
  {
    final String path = makeAsyncQueryStatePath(asyncResultId);
    final byte[] bytes;
    SqlAsyncQueryMetadata metadata = null;
    try {
      if (stat != null) {
        bytes = curator.getData().storingStatIn(stat).forPath(path);
        metadata = new SqlAsyncQueryMetadata(stat.getMtime());
      } else {
        bytes = curator.getData().forPath(path);
      }
    }
    catch (KeeperException.NoNodeException e) {
      return Optional.empty();
    }
    catch (Exception e) {
      throw new IOE(e, "Error while retrieving data from path at [%s]", path);
    }

    SqlAsyncQueryDetails sqlAsyncQueryDetails = jsonMapper.readValue(bytes, SqlAsyncQueryDetails.class);
    return Optional.of(new SqlAsyncQueryDetailsAndMetadata(sqlAsyncQueryDetails, metadata));
  }

  @Override
  public Collection<String> getAllAsyncResultIds() throws IOException
  {
    final String path = makeAsyncRootPath();
    List<String> asyncResultIds;
    try {
      asyncResultIds = curator.getChildren().forPath(path);
    }
    catch (KeeperException.NoNodeException e) {
      return ImmutableList.of();
    }
    catch (Exception e) {
      throw new IOE(e, "Error while retrieving children from path at [%s]", path);
    }
    return Collections.unmodifiableCollection(asyncResultIds);
  }

  @Override
  public long totalCompleteQueryResultsSize() throws IOException
  {
    return totalCompleteQueryResultsSize(getAllAsyncResultIds());
  }

  @Override
  public long totalCompleteQueryResultsSize(Collection<String> asyncResultIds) throws IOException
  {
    long sum = 0L;
    for (String asyncResultId : asyncResultIds) {
      sum += getQueryDetails(asyncResultId).filter(queryDetails -> queryDetails.getState() == State.COMPLETE)
                                           .map(SqlAsyncQueryDetails::getResultLength)
                                           .orElse(0L);
    }
    return sum;
  }

  private String makeAsyncQueryStatePath(final String asyncResultId)
  {
    // TODO(gianm): Configurable PATH? But why...
    return ZKPaths.makePath(makeAsyncRootPath(), SqlAsyncUtil.safeId(asyncResultId));
  }

  private String makeAsyncRootPath()
  {
    // TODO(gianm): Configurable PATH? But why...
    return ZKPaths.makePath(config.getBase(), PATH);
  }
}
