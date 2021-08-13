/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Optional;

public class CuratorSqlAsyncMetadataManager implements SqlAsyncMetadataManager
{
  private static final String PATH = "sql-async-query";

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
    final String path = makePath(queryDetails.getAsyncResultId());
    final byte[] payload = jsonMapper.writeValueAsBytes(queryDetails);

    try {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
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
    final String path = makePath(queryDetails.getAsyncResultId());
    final byte[] payload = jsonMapper.writeValueAsBytes(queryDetails);

    try {
      curator.setData().forPath(path, payload);
    }
    catch (KeeperException.NoNodeException e) {
      throw new AsyncQueryDoesNotExistException(queryDetails.getAsyncResultId());
    }
    catch (Exception e) {
      throw new IOE(e, "Error while updating path at [%s]", path);
    }
  }

  @Override
  public boolean removeQueryDetails(final SqlAsyncQueryDetails queryDetails) throws IOException
  {
    final String path = makePath(queryDetails.getAsyncResultId());

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
    final String path = makePath(asyncResultId);
    final byte[] bytes;

    try {
      bytes = curator.getData().forPath(path);
    }
    catch (KeeperException.NoNodeException e) {
      return Optional.empty();
    }
    catch (Exception e) {
      throw new IOE(e, "Error while retrieving data from path at [%s]", path);
    }

    return Optional.of(jsonMapper.readValue(bytes, SqlAsyncQueryDetails.class));
  }

  private String makePath(final String sqlQueryId)
  {
    // TODO(gianm): Configurable PATH? But why...
    return ZKPaths.makePath(config.getBase(), PATH, SqlAsyncUtil.safeId(sqlQueryId));
  }
}
