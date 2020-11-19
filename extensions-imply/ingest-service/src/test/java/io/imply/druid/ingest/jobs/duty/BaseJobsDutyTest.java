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
import com.google.common.collect.ImmutableList;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobProcessingContext;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.sql.IngestServiceSqlMetadataStore;
import io.imply.druid.ingest.metadata.sql.IngestServiceSqlMetatadataConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class BaseJobsDutyTest
{
  static final String TABLE = "saasy-table";
  static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  final IngestSchema ingestSchema = new IngestSchema(
      new TimestampSpec("time", "iso", null),
      new DimensionsSpec(
          ImmutableList.of(
              StringDimensionSchema.create("column1"),
              StringDimensionSchema.create("column2")
          )
      ),
      new JsonInputFormat(null, null, null)
  );

  FileStore fileStore;
  IngestServiceMetadataStore metadataStore;
  IndexingServiceClient indexingServiceClient;
  CoordinatorClient coordinatorClient;
  JobProcessingContext jobProcessingContext;


  @Before
  public void setup()
  {
    metadataStore = new IngestServiceSqlMetadataStore(
        () -> IngestServiceSqlMetatadataConfig.DEFAULT_CONFIG,
        derbyConnectorRule.getConnector(),
        MAPPER
    );
    fileStore = EasyMock.createMock(FileStore.class);
    indexingServiceClient = EasyMock.createMock(IndexingServiceClient.class);
    coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    jobProcessingContext = new JobProcessingContext(indexingServiceClient, coordinatorClient, metadataStore, fileStore, MAPPER);
  }

  @After
  public void teardown()
  {
    verifyAll();
  }

  void verifyAll()
  {
    EasyMock.verify(fileStore, coordinatorClient, indexingServiceClient);
  }

  void replayAll()
  {
    EasyMock.replay(fileStore, coordinatorClient, indexingServiceClient);
  }

  void resetAll()
  {
    EasyMock.reset(fileStore, coordinatorClient, indexingServiceClient);
  }
}
