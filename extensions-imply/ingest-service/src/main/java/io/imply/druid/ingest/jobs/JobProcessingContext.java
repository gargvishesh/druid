/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;

/**
 * Useful facilities for {@link JobRunner} to make use of while processing jobs.
 */
public class JobProcessingContext
{
  private final IndexingServiceClient indexingClient;
  private final CoordinatorClient coordinatorClient;
  private final IngestServiceMetadataStore metadataStore;
  private final FileStore fileStore;

  private final ObjectMapper jsonMapper;

  public JobProcessingContext(
      IndexingServiceClient indexingClient,
      CoordinatorClient coordinatorClient,
      IngestServiceMetadataStore metadataStore,
      FileStore fileStore,
      ObjectMapper jsonMapper
  )
  {
    this.indexingClient = indexingClient;
    this.coordinatorClient = coordinatorClient;
    this.metadataStore = metadataStore;
    this.fileStore = fileStore;
    this.jsonMapper = jsonMapper;
  }

  public CoordinatorClient getCoordinatorClient()
  {
    return coordinatorClient;
  }

  public IngestServiceMetadataStore getMetadataStore()
  {
    return metadataStore;
  }

  public FileStore getFileStore()
  {
    return fileStore;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public IndexingServiceClient getIndexingClient()
  {
    return indexingClient;
  }
}
