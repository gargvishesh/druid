/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.ingest.client.IngestServiceClient;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.local.LocalFileStore;
import io.imply.druid.ingest.files.local.LocalFileStoreConfig;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.server.IngestJobRequest;
import io.imply.druid.ingest.server.IngestJobsResponse;
import io.imply.druid.ingest.server.StageBatchAppendPushIngestJobResponse;
import io.imply.druid.ingest.server.TablesResponse;
import io.imply.druid.tests.ImplyTestNGGroup;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = ImplyTestNGGroup.INGEST_SERVICE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIngestServiceTest
{
  private static final Logger LOG = new Logger(ITIngestServiceTest.class);
  private static final String EVENT_DATA_FILE = "/data/batch_index/json/wikipedia_index_data3.json";
  private static final String TABLE_NAME_TEMPLATE = "wiki-saas-%s";

  @Inject
  ObjectMapper objectMapper;

  @Inject
  IntegrationTestingConfig testConfig;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  DruidNodeDiscoveryProvider druidNodeDiscovery;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  IngestServiceTenantConfig tenantConfig;
  IngestServiceClient client;
  String tableName;
  LocalFileStore localFileStore;

  @BeforeClass
  public void setup()
  {
    this.tenantConfig = new IngestServiceTenantConfig()
    {
      @Override
      public String getAccountId()
      {
        return "imply";
      }

      @Override
      public String getClusterId()
      {
        return "integration-test-imply";
      }
    };
    this.client = new ITIngestServiceClient(httpClient, druidNodeDiscovery, objectMapper, testConfig);
    this.tableName = StringUtils.format(TABLE_NAME_TEMPLATE, UUIDUtils.generateUuid());

    LocalFileStoreConfig fileStoreConfig = new LocalFileStoreConfig()
    {
      @Override
      public String getBaseDir()
      {
        return StringUtils.format(
            "%s/shared/imply/ingest",
            System.getenv("HOME")
        );
      }
    };
    this.localFileStore = new LocalFileStore(tenantConfig, fileStoreConfig, objectMapper);
  }

  @Test
  public void testSimpleEndToEnd() throws IOException, ExecutionException, InterruptedException
  {
    ITRetryUtil.retryUntil(
        () -> client.isSelfDiscovered(),
        true,
        TimeUnit.SECONDS.toMillis(5),
        60,
        "ingest-service discovered"
    );

    TablesResponse tables = client.getTables();
    LOG.info("Got tables: %s", tables);
    Assert.assertNotNull(tables);

    LOG.info("creating table %s", tableName);
    Assert.assertTrue(client.createTable(tableName));

    tables = client.getTables();
    LOG.info("Got tables: %s", tables);
    Assert.assertTrue(tables.getTables().size() >= 1);

    LOG.info("staging job");
    String stageResponseString = client.stageJob(tableName, new BatchAppendJobRunner());
    StageBatchAppendPushIngestJobResponse stageResponse = objectMapper.readValue(
        stageResponseString,
        StageBatchAppendPushIngestJobResponse.class
    );

    Assert.assertNotNull(stageResponse.getJobId());
    Assert.assertNotNull(stageResponse.getDropoffUri());
    LOG.info("Staged job: %s", stageResponse.getJobId());

    URI adjusted = localFileStore.makeDropoffUri(stageResponse.getJobId());
    LOG.info("Copying ingest data to: %s", adjusted);
    byte[] buffer = new byte[1024];
    File ingestDropoff = new File(adjusted);
    ingestDropoff.getParentFile().mkdirs();
    long wrote = FileUtils.copyLarge(
        this.getClass().getResourceAsStream(EVENT_DATA_FILE),
        ingestDropoff,
        buffer,
        Predicates.alwaysFalse(),
        1,
        StringUtils.format("Failed to copy to %s", adjusted)
    );

    Assert.assertTrue(wrote > 0);

    IngestJobsResponse jobs = client.getJobs();
    Assert.assertTrue(jobs.getJobs().size() >= 1);

    IngestJobRequest jobRequest = new IngestJobRequest(
        new IngestSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("page"))),
            null,
            new JsonInputFormat(null, null, null),
            null
        ),
        null
    );

    SamplerResponse samplerResponse = client.sampleJob(tableName, stageResponse.getJobId(), jobRequest);


    LOG.info("Got sample response with %s rows", samplerResponse.getData().size());
    LOG.info(objectMapper.writeValueAsString(samplerResponse));
    Assert.assertTrue(samplerResponse.getData().size() > 0);
    Assert.assertTrue(samplerResponse.getData().get(0).getParsed().containsKey("page"));
    Assert.assertFalse(samplerResponse.getData().get(0).getParsed().containsKey("language"));

    jobRequest = new IngestJobRequest(
        new IngestSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("page"), StringDimensionSchema.create("language"))),
            null,
            new JsonInputFormat(null, null, null),
            null
        ),
        null
    );

    // sampling again with different schema
    samplerResponse = client.sampleJob(tableName, stageResponse.getJobId(), jobRequest);

    LOG.info("Got sample response with %s rows", samplerResponse.getData().size());
    LOG.info(objectMapper.writeValueAsString(samplerResponse));
    Assert.assertTrue(samplerResponse.getData().size() > 0);
    Assert.assertTrue(samplerResponse.getData().get(0).getParsed().containsKey("page"));
    Assert.assertTrue(samplerResponse.getData().get(0).getParsed().containsKey("language"));

    // sampling again with same schema just for funsies
    samplerResponse = client.sampleJob(tableName, stageResponse.getJobId(), jobRequest);

    LOG.info("Got sample response with %s rows", samplerResponse.getData().size());
    LOG.info(objectMapper.writeValueAsString(samplerResponse));
    Assert.assertTrue(samplerResponse.getData().size() > 0);
    Assert.assertTrue(samplerResponse.getData().get(0).getParsed().containsKey("page"));
    Assert.assertTrue(samplerResponse.getData().get(0).getParsed().containsKey("language"));

    LOG.info("Submitting job %s", stageResponse.getJobId());

    Assert.assertTrue(client.submitJob(tableName, stageResponse.getJobId(), jobRequest));

    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded(tableName), "segment loaded"
    );
  }

  private static class ITIngestServiceClient extends IngestServiceClient
  {
    private final IntegrationTestingConfig testConfig;

    public ITIngestServiceClient(
        HttpClient client,
        DruidNodeDiscoveryProvider discoveryProvider,
        ObjectMapper jsonMapper,
        IntegrationTestingConfig testConfig
    )
    {
      super(client, discoveryProvider, jsonMapper);
      this.testConfig = testConfig;
    }

    @Override
    protected DiscoveryDruidNode pickNode()
    {
      DiscoveryDruidNode picked = super.pickNode();
      if (testConfig.isDocker()) {
        DruidNode discoNode = picked.getDruidNode();
        DruidNode copy = new DruidNode(
            discoNode.getServiceName(),
            testConfig.getDockerHost(),
            discoNode.isBindOnHost(),
            discoNode.getPlaintextPort(),
            discoNode.getTlsPort(),
            discoNode.isEnablePlaintextPort(),
            discoNode.isEnableTlsPort()
        );
        return new DiscoveryDruidNode(copy, picked.getNodeRole(), picked.getServices());
      }
      return picked;
    }
  }
}
