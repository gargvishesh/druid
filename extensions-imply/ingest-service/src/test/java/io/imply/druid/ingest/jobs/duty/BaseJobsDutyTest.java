/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobProcessingContext;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.OverlordClient;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

public abstract class BaseJobsDutyTest
{
  static final String JOB_ID = "somejobid";

  static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  FileStore fileStore;
  IngestServiceMetadataStore metadataStore;
  OverlordClient indexingServiceClient;
  CoordinatorClient coordinatorClient;
  JobProcessingContext jobProcessingContext;

  IngestJob job;
  JobRunner runner;

  boolean shouldVerify = false;

  @Before
  public void setup()
  {
    metadataStore = EasyMock.createMock(IngestServiceMetadataStore.class);
    fileStore = EasyMock.createMock(FileStore.class);
    indexingServiceClient = EasyMock.createMock(OverlordClient.class);
    coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    jobProcessingContext = new JobProcessingContext(indexingServiceClient, coordinatorClient, metadataStore, fileStore, MAPPER);
    job = EasyMock.createMock(IngestJob.class);
    runner = EasyMock.createMock(JobRunner.class);
  }

  @After
  public void teardown()
  {
    if (shouldVerify) {
      verifyAll();
    }
  }

  void verifyAll()
  {
    EasyMock.verify(fileStore, coordinatorClient, indexingServiceClient, job, runner, metadataStore);
  }

  void replayAll()
  {
    shouldVerify = true;
    EasyMock.replay(fileStore, coordinatorClient, indexingServiceClient, job, runner, metadataStore);
  }
}
