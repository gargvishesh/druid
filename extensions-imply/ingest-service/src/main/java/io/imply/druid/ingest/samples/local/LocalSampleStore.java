/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.samples.SampleStore;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

public class LocalSampleStore implements SampleStore
{
  private final ObjectMapper jsonMapper;
  private final IngestServiceTenantConfig serviceConfig;
  private final LocalSampleStoreConfig sampleStoreConfig;

  @Inject
  public LocalSampleStore(
      IngestServiceTenantConfig serviceConfig,
      LocalSampleStoreConfig sampleStoreConfig,
      @Json ObjectMapper jsonMapper
  )
  {
    this.serviceConfig = serviceConfig;
    this.sampleStoreConfig = sampleStoreConfig;
    this.jsonMapper = jsonMapper;
  }

  @Nullable
  @Override
  public SamplerResponse getSamplerResponse(String jobId) throws IOException
  {
    File samplerResponseFile = makeFile(jobId);
    if (samplerResponseFile.exists()) {
      return jsonMapper.readValue(
          samplerResponseFile,
          SamplerResponse.class
      );
    } else {
      return null;
    }
  }

  @Override
  public void storeSample(String jobId, SamplerResponse samplerResponse)
  {
    makeSampleDir();

    File samplerResponseFile = makeFile(jobId);
    try {
      jsonMapper.writeValue(
          samplerResponseFile,
          samplerResponse
      );
    }
    catch (IOException ioe) {
      throw new RuntimeException("Encountered IOException for job: " + jobId, ioe);
    }
  }

  @Override
  public void deleteSample(String jobId)
  {
    File samplerResponseFile = makeFile(jobId);
    samplerResponseFile.delete();
  }

  private File makeFile(String jobId)
  {
    return new File(makeFullPath(jobId));
  }

  private String makeFullPath(String jobId)
  {
    return StringUtils.format(
        "%s/%s",
        sampleStoreConfig.getBaseDir(),
        getSampleFileSubPath(jobId)
    );
  }

  private String getSampleFileSubPath(String jobId)
  {
    return StringUtils.format("%s/%s.json", getDirSubPath(), jobId);
  }

  private String getDirSubPath()
  {
    return StringUtils.format("/%s/%s/samples", serviceConfig.getAccountId(), serviceConfig.getClusterId());
  }

  private void makeSampleDir()
  {
    File sampleDir = new File(
        StringUtils.format(
            "%s%s",
            sampleStoreConfig.getBaseDir(),
            getDirSubPath()
        )
    );
    sampleDir.mkdirs();
  }
}
