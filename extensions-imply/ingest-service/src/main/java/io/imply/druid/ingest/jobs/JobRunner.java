/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = BatchAppendJobRunner.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "batch_append", value = BatchAppendJobRunner.class)
})
public interface JobRunner
{
  /**
   * Start an {@link IngestJob}, such as submitting a batch ingestion task to a Druid overlord, but really could be
   * anything.
   */
  boolean startJob(JobProcessingContext jobProcessingContext, IngestJob job);

  /**
   * Perform any actions necessary to update job status for {@link IngestJob} in the {@link IngestServiceMetadataStore}
   * (available via {@link JobProcessingContext#getMetadataStore()}).
   * @return
   */
  JobUpdateStateResult updateJobStatus(JobProcessingContext jobProcessingContext, IngestJob job);
}
