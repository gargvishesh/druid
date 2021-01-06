/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.samples.SampleStore;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;

public class S3SampleStore implements SampleStore
{
  private final IngestServiceTenantConfig serviceConfig;
  private final S3SampleStoreConfig s3Config;
  private final AmazonS3 s3;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3SampleStore(
      IngestServiceTenantConfig serviceConfig,
      S3SampleStoreConfig s3Config,
      AmazonS3 s3Client,
      @Json ObjectMapper jsonMapper
  )
  {
    this.serviceConfig = serviceConfig;
    this.s3Config = s3Config;
    this.s3 = s3Client;
    this.jsonMapper = jsonMapper;
  }

  @Nullable
  @Override
  public SamplerResponse getSamplerResponse(String jobId) throws IOException
  {
    String objectPath = makeObjectPath(jobId);

    if (!s3.doesObjectExist(s3Config.getBucket(), objectPath)) {
      return null;
    }

    S3Object s3Object = s3.getObject(s3Config.getBucket(), makeObjectPath(jobId));
    return jsonMapper.readValue(
        s3Object.getObjectContent(),
        SamplerResponse.class
    );
  }

  @Override
  public void storeSample(String jobId, SamplerResponse samplerResponse)
  {
    try {
      s3.putObject(
          s3Config.getBucket(),
          makeObjectPath(jobId),
          jsonMapper.writeValueAsString(samplerResponse)
      );
    }
    catch (JsonProcessingException jpe) {
      throw new RuntimeException(jpe);
    }
  }

  @Override
  public void deleteSample(String jobId)
  {
    s3.deleteObject(
        s3Config.getBucket(),
        makeObjectPath(jobId)
    );
  }

  private String makeObjectPath(String jobId)
  {
    return StringUtils.format("%s/%s/samples/%s.json", serviceConfig.getAccountId(), serviceConfig.getClusterId(), jobId);
  }
}
