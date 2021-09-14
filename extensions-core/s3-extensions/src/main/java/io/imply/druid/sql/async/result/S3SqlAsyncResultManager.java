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

package io.imply.druid.sql.async.result;

import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.sql.async.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.SqlAsyncModule;
import io.imply.druid.sql.async.SqlAsyncQueryDetails;
import io.imply.druid.sql.async.SqlAsyncResultManager;
import io.imply.druid.sql.async.SqlAsyncResults;
import io.imply.druid.storage.s3.ImplyServerSideEncryptingAmazonS3;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.utils.Streams;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class S3SqlAsyncResultManager implements SqlAsyncResultManager
{
  private final ImplyServerSideEncryptingAmazonS3 s3Client;
  private final S3SqlAsyncResultManagerConfig config;
  private final SqlAsyncMetadataManager metadataManager;

  @Inject
  public S3SqlAsyncResultManager(
      ImplyServerSideEncryptingAmazonS3 s3Client,
      S3SqlAsyncResultManagerConfig config,
      SqlAsyncMetadataManager metadataManager
  ) throws IOException
  {
    if (Strings.isNullOrEmpty(config.getBucket())) {
      throw new ISE("Property '%s.s3.bucket' is required", SqlAsyncModule.BASE_STORAGE_CONFIG_KEY);
    }
    if (Strings.isNullOrEmpty(config.getPrefix())) {
      throw new ISE("Property '%s.s3.prefix' is required", SqlAsyncModule.BASE_STORAGE_CONFIG_KEY);
    }

    this.s3Client = s3Client;
    this.config = config;
    this.metadataManager = metadataManager;

    FileUtils.forceMkdir(config.getTempDir());
  }

  @Override
  public OutputStream writeResults(SqlAsyncQueryDetails queryDetails) throws IOException
  {
    return RetriableS3OutputStream.create(config, metadataManager, s3Client, queryDetails);
  }

  @Override
  public Optional<SqlAsyncResults> readResults(SqlAsyncQueryDetails queryDetails)
  {
    final String key = RetriableS3OutputStream.getS3KeyForQuery(config.getPrefix(), queryDetails.getAsyncResultId());
    final S3Object object = s3Client.getObject(config.getBucket(), key);
    if (object != null) {
      return Optional.of(new SqlAsyncResults(object.getObjectContent(), queryDetails.getResultLength()));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public boolean deleteResults(String asyncResultId)
  {
    final String key = RetriableS3OutputStream.getS3KeyForQuery(config.getPrefix(), asyncResultId);
    s3Client.deleteObject(config.getBucket(), key);
    return true;
  }

  @Override
  public Collection<String> getAllAsyncResultIds()
  {
    final URI uri = new CloudObjectLocation(config.getBucket(), config.getPrefix()).toUri(S3StorageDruidModule.SCHEME);
    return Streams.sequentialStreamFrom(S3Utils.objectSummaryIterator(s3Client, ImmutableList.of(uri), 10000))
                  .map(summary -> summary.getKey().substring(config.getPrefix().length() + 1)) // + 1 to eliminate a slash
                  .collect(Collectors.toList());
  }
}
