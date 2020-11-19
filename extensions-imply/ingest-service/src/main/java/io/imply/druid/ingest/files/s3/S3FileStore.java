/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.BaseFileStore;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

public class S3FileStore extends BaseFileStore
{
  private final IngestServiceTenantConfig serviceConfig;
  private final S3FileStoreConfig s3Config;
  private final AmazonS3 s3;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3FileStore(
      IngestServiceTenantConfig serviceConfig,
      S3FileStoreConfig s3Config,
      AmazonS3 s3Client,
      @Json ObjectMapper jsonMapper
  )
  {
    super(jsonMapper);
    this.serviceConfig = serviceConfig;
    this.s3Config = s3Config;
    this.s3 = s3Client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void touchFile(String file)
  {
    // reset object expiration by copying? maybe there should be some logic to check existing age, and
    // config i guess could supply whatever the expiry policy is set to so that we only do this if files are getting
    // close to expiring
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean fileExists(String file)
  {
    return s3.doesObjectExist(s3Config.getBucket(), makeObjectPath(file));
  }

  @Override
  public void deleteFile(String file)
  {
    s3.deleteObject(s3Config.getBucket(), makeObjectPath(file));
  }

  @Override
  public URI makeDropoffUri(String file)
  {
    URL url = s3.generatePresignedUrl(s3Config.getBucket(), makeObjectPath(file), DateTimes.nowUtc().plusDays(1).toDate());
    try {
      return url.toURI();
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Object> makeInputSourceMap(String file)
  {
    return ImmutableMap.of(
        "type", S3FileStoreModule.TYPE,
        "uris", ImmutableList.of(StringUtils.format("s3://%s", makeObjectPath(file)))
    );
  }

  private String makeObjectPath(String file)
  {
    return StringUtils.format("%s/%s/files/%s", serviceConfig.getAccountId(), serviceConfig.getClusterId(), file);
  }
}
