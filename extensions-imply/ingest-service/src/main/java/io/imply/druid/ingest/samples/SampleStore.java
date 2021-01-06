/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples;

import org.apache.druid.client.indexing.SamplerResponse;

import javax.annotation.Nullable;
import java.io.IOException;

public interface SampleStore
{
  String STORE_PROPERTY_BASE = "imply.ingest.sampleStore";

  @Nullable
  SamplerResponse getSamplerResponse(String jobId) throws IOException;

  void storeSample(String jobId, SamplerResponse samplerResponse);

  void deleteSample(String jobId);
}
