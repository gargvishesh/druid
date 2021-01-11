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

  /**
   * Returns a sampled version of the data associated with a {@link io.imply.druid.ingest.metadata.IngestJob}, if
   * available, or null if the sample is not (the implication that the actual input source will need sampled)
   */
  @Nullable
  SamplerResponse getSamplerResponse(String jobId) throws IOException;

  /**
   * Stores sampled data which may be used to speed up the sampling workflow for the input data of an
   * {@link io.imply.druid.ingest.metadata.IngestJob}. If the cached sample is available, an
   * {@link org.apache.druid.query.InlineDataSource} can be constructed and contain the sampled data rather than using
   * the actual full {@link org.apache.druid.data.input.InputSource} from deep storage.
   */
  void storeSample(String jobId, SamplerResponse samplerResponse);

  /**
   * Remove a sample for a {@link io.imply.druid.ingest.metadata.IngestJob} from the sample store
   */
  void deleteSample(String jobId);
}
