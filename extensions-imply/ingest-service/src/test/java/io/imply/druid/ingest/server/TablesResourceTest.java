/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.OverlordClient;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.JobScheduleException;
import io.imply.druid.ingest.metadata.PartitionScheme;
import io.imply.druid.ingest.metadata.Table;
import io.imply.druid.ingest.samples.SampleStore;
import io.imply.druid.ingest.samples.local.LocalSampleStore;
import io.imply.druid.ingest.samples.local.LocalSampleStoreConfig;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TablesResourceTest
{
  private static final String TABLE = "test";
  private static final IngestSchema TEST_SCHEMA = new IngestSchema(
      new TimestampSpec("time", "iso", null),
      new DimensionsSpec(
          ImmutableList.of(
              StringDimensionSchema.create("x"),
              StringDimensionSchema.create("y")
          )
      ),
      new PartitionScheme(Granularities.DAY, null),
      new JsonInputFormat(null, null, null),
      "test schema"
  );

  private final IngestServiceTenantConfig tenantConfig = new IngestServiceTenantConfig()
  {
    @Override
    public String getAccountId()
    {
      return "test-account";
    }

    @Override
    public String getClusterId()
    {
      return "test-cluster-1";
    }
  };

  private static final DateTime CREATED = DateTimes.nowUtc();
  private static final List<Table> TABLE_LIST_WRITE = ImmutableList.of(
      new Table("foo1", CREATED),
      new Table("foo2", CREATED),
      new Table("foo3", CREATED),
      new Table("foo4", CREATED),
      new Table("foo5", CREATED),
      new Table("foo6", CREATED)
  );
  private static final List<Table> TABLE_LIST_READ = ImmutableList.of(
      new Table("bar1", CREATED),
      new Table("bar2", CREATED),
      new Table("bar3", CREATED)
  );
  private static final List<Table> TABLE_LIST_UNAUTHORIZED = ImmutableList.of(
      new Table("crook1", CREATED),
      new Table("crook2", CREATED),
      new Table("crook3", CREATED)
  );
  private static final List<Table> TABLE_LIST = ImmutableList.<Table>builder()
                                                             .addAll(TABLE_LIST_READ)
                                                             .addAll(TABLE_LIST_WRITE)
                                                             .addAll(TABLE_LIST_UNAUTHORIZED)
                                                             .build();

  private static final List<String> TABLE_NAMES_WRITE =
      TABLE_LIST_WRITE.stream().map(Table::getName).collect(Collectors.toList());
  private static final List<String> TABLE_NAMES_READ =
      TABLE_LIST_READ.stream().map(Table::getName).collect(Collectors.toList());
  private static final List<String> TABLE_NAMES_UNAUTHORIZED =
      TABLE_LIST_UNAUTHORIZED.stream().map(Table::getName).collect(Collectors.toList());

  private FileStore fileStore;
  private IngestServiceMetadataStore metadataStore;
  private HttpServletRequest req;
  private TablesResource tablesResource;
  private SampleStore sampleStore;
  private OverlordClient overlordClient;
  private ObjectMapper jsonMapper;

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();

    fileStore = EasyMock.createMock(FileStore.class);
    metadataStore = EasyMock.createMock(IngestServiceMetadataStore.class);
    req = EasyMock.createMock(HttpServletRequest.class);

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (authenticationResult, resource, action) -> {
          if (TABLE_NAMES_WRITE.contains(resource.getName()) && action == Action.WRITE) {
            return new Access(true);
          } else if (TABLE_NAMES_READ.contains(resource.getName()) && action == Action.READ) {
            return new Access(true);
          } else {
            return new Access(false);
          }
        };
      }
    };
    overlordClient = EasyMock.createMock(OverlordClient.class);

    File tmpDir = FileUtils.createTempDir();
    sampleStore = new LocalSampleStore(
        tenantConfig,
        new LocalSampleStoreConfig(tmpDir.getAbsolutePath()),
        jsonMapper
    );
    tablesResource = new TablesResource(
        tenantConfig,
        authMapper,
        fileStore,
        metadataStore,
        sampleStore,
        overlordClient,
        jsonMapper
    );
  }

  @After
  public void teardown()
  {
    EasyMock.verify(fileStore, metadataStore, req);
  }

  @Test
  public void testStageJobShoulWorkWhenTableExists() throws URISyntaxException
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();
    EasyMock.expect(metadataStore.druidTableExists(EasyMock.eq(TABLE))).andReturn(true).once();
    EasyMock.expect(metadataStore.stageJob(EasyMock.eq(TABLE), EasyMock.anyObject(JobRunner.class)))
            .andReturn(id)
            .once();
    String uri = StringUtils.format("http://127.0.0.1/some/path/%s", id);
    EasyMock.expect(fileStore.makeDropoffUri(id))
            .andReturn(new URI(uri))
            .once();
    EasyMock.replay(fileStore, metadataStore, req);

    Response response = tablesResource.stageIngestJob(null, TABLE);

    Map<String, Object> responseEntity = (Map<String, Object>) response.getEntity();
    Assert.assertEquals(id, responseEntity.get("jobId"));
    Assert.assertEquals(new URI(uri), responseEntity.get("dropoffUri"));

    Assert.assertEquals(200, response.getStatus());
  }


  @Test
  public void testStageJobShoulReturn404WhenTableDoesNotExist()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    EasyMock.expect(metadataStore.druidTableExists(EasyMock.eq(TABLE))).andReturn(false).once();

    EasyMock.replay(fileStore, metadataStore, req);

    Response response = tablesResource.stageIngestJob(null, TABLE);

    Assert.assertEquals(404, response.getStatus());
  }


  @Test
  public void testScheduleIngestJobShouldSucceedWhenJobExists()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(TEST_SCHEMA).atLeastOnce();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.scheduleJob(id, TEST_SCHEMA)).andReturn(1).once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertTrue(response.getMetadata().containsKey("Location"));
    Assert.assertEquals(201, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldSucceedWhenJobAndSchemaIdExists()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(null).atLeastOnce();
    EasyMock.expect(scheduleRequest.getSchemaId()).andReturn(1).atLeastOnce();

    EasyMock.expect(metadataStore.schemaExists(1)).andReturn(true).once();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.scheduleJob(id, 1)).andReturn(1).once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertTrue(response.getMetadata().containsKey("Location"));
    Assert.assertEquals(201, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldFailWhenJobDoesNotExist()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(TEST_SCHEMA).atLeastOnce();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.scheduleJob(id, TEST_SCHEMA)).andReturn(0).once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(404, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }


  @Test
  public void testScheduleIngestJobShouldFailWhenSchemaNotExist()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(null).atLeastOnce();
    EasyMock.expect(scheduleRequest.getSchemaId()).andReturn(1).atLeastOnce();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.schemaExists(1)).andReturn(false).atLeastOnce();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        ImmutableMap.of("error", StringUtils.format("schema [1] does not exist, cannot schedule job [%s]", id)),
        response.getEntity()
    );
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldFailWhenJobDoesNotMatchTable()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn("another_table").once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        ImmutableMap.of("error", StringUtils.format("job [%s] does not belong to table [test]", id)),
        response.getEntity()
    );
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldFailWhenTooManyJobsForSameJobIdExist()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(TEST_SCHEMA).atLeastOnce();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.scheduleJob(id, TEST_SCHEMA)).andReturn(10).once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(500, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldFailWhenJobInBadJobState()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(TEST_SCHEMA).atLeastOnce();
    EasyMock.expect(metadataStore.getJobTable(id)).andReturn(TABLE).once();
    EasyMock.expect(metadataStore.scheduleJob(id, TEST_SCHEMA)).andThrow(new JobScheduleException(id, JobState.RUNNING)).once();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        ImmutableMap.of("error", StringUtils.format("Cannot schedule job [%s] because it is in [RUNNING] state", id)),
        response.getEntity()
    );
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testTablesInsert()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.insertTable(TABLE)).andReturn(1).once();
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.createTable(TABLE, req);

    Assert.assertEquals(201, response.getStatus());
    Assert.assertTrue(response.getMetadata().size() > 0);
    Assert.assertTrue(response.getMetadata().containsKey("Location"));
  }

  @Test
  public void testTablesInsertServerError()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.insertTable(TABLE)).andThrow(new RuntimeException("some server error"));
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.createTable(TABLE, req);

    Assert.assertEquals(500, response.getStatus());
    Map<String, Object> responseEntity = (Map<String, Object>) response.getEntity();
    Assert.assertTrue(responseEntity.get("error").toString().length() > 0);
  }


  @Test
  public void testTablesGetShouldReturnAllTablesWithNoJobs()
  {
    expectAuthorizationTokenCheck();
    Map<Table, Object2IntMap<JobState>> tableSummaries = new HashMap<>();
    for (Table t : TABLE_LIST) {
      tableSummaries.put(t, null);
    }
    Set<TableInfo> expectedTablesSet = getExpectedTables(tableSummaries);
    List<TableInfo> expectedTablesList = new ArrayList<>(expectedTablesSet);
    expectedTablesList.sort(Comparator.comparing(TableInfo::getName));

    EasyMock.expect(metadataStore.getTableJobSummary(Collections.emptySet()))
            .andReturn(tableSummaries)
            .once();
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("NONE", req);
    TablesResponse tablesResponse = (TablesResponse) response.getEntity();

    Assert.assertEquals(expectedTablesList, tablesResponse.getTables());

    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(metadataStore, req, fileStore);
  }

  @Test
  public void testTablesGetShouldReturnOnlyTablesWithRunningState()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Map<Table, Object2IntMap<JobState>> tableSummaries = new HashMap<>();
    for (Table t : TABLE_LIST) {
      if (t.getName().equals(TABLE_NAMES_WRITE.get(0))
          || t.getName().equals(TABLE_NAMES_WRITE.get(1))
          || t.getName().equals(TABLE_NAMES_READ.get(2))) {
        Object2IntMap<JobState> tableJobStates = new Object2IntOpenHashMap<>();
        tableJobStates.put(JobState.RUNNING, i.addAndGet(1));
        tableSummaries.put(t, tableJobStates);
      }
    }
    List<TableInfo> expectedTablesList = new ArrayList<>(getExpectedTables(tableSummaries));
    expectedTablesList.sort(Comparator.comparing(TableInfo::getName));

    EasyMock.expect(metadataStore.getTableJobSummary(Collections.singleton(JobState.RUNNING)))
            .andReturn(tableSummaries);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("RUNNING", req);
    TablesResponse tablesResponse = (TablesResponse) response.getEntity();

    Assert.assertEquals(expectedTablesList, tablesResponse.getTables());

    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(metadataStore, req, fileStore);
  }

  @Test
  public void testTablesGetShouldReturnAllTablesWithOrWithoutJobs()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Map<Table, Object2IntMap<JobState>> tableSummaries = new HashMap<>();
    for (Table t : TABLE_LIST) {
      Object2IntMap<JobState> tableJobStates = new Object2IntOpenHashMap<>();
      if (TABLE_NAMES_WRITE.get(0).equals(t.getName())) {
        tableJobStates.put(JobState.STAGED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(1).equals(t.getName())) {
        tableJobStates.put(JobState.RUNNING, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(2).equals(t.getName())) {
        tableJobStates.put(JobState.SCHEDULED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(3).equals(t.getName())) {
        tableJobStates.put(JobState.CANCELLED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(4).equals(t.getName())) {
        tableJobStates.put(JobState.COMPLETE, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(5).equals(t.getName())) {
        tableJobStates.put(JobState.FAILED, i.addAndGet(1));
      }
      tableSummaries.put(t, tableJobStates);
    }
    List<TableInfo> expectedTablesList = new ArrayList<>(getExpectedTables(tableSummaries));
    expectedTablesList.sort(Comparator.comparing(TableInfo::getName));

    EasyMock.expect(metadataStore.getTableJobSummary(Collections.singleton(JobState.RUNNING)))
            .andReturn(tableSummaries);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("RUNNING", req);
    TablesResponse tablesResponse = (TablesResponse) response.getEntity();

    Assert.assertEquals(expectedTablesList, tablesResponse.getTables());

    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(metadataStore, req, fileStore);
  }

  @Test
  public void testTablesGetShouldNotReturnUnauthorizedTables()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Map<Table, Object2IntMap<JobState>> tableSummaries = new HashMap<>();
    for (Table t : TABLE_LIST) {
      Object2IntMap<JobState> tableJobStates = new Object2IntOpenHashMap<>();
      if (TABLE_NAMES_WRITE.get(0).equals(t.getName())) {
        tableJobStates.put(JobState.STAGED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(1).equals(t.getName())) {
        tableJobStates.put(JobState.RUNNING, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(2).equals(t.getName())) {
        tableJobStates.put(JobState.SCHEDULED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(3).equals(t.getName())) {
        tableJobStates.put(JobState.CANCELLED, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(4).equals(t.getName())) {
        tableJobStates.put(JobState.COMPLETE, i.addAndGet(1));
      } else if (TABLE_NAMES_WRITE.get(5).equals(t.getName())) {
        tableJobStates.put(JobState.FAILED, i.addAndGet(1));
      }
      tableSummaries.put(t, tableJobStates);
    }

    List<TableInfo> expectedTablesList = new ArrayList<>(getExpectedTables(tableSummaries));
    expectedTablesList.sort(Comparator.comparing(TableInfo::getName));

    EasyMock.expect(metadataStore.getTableJobSummary(null))
            .andReturn(tableSummaries);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("ALL", req);
    TablesResponse tablesResponse = (TablesResponse) response.getEntity();

    Assert.assertEquals(expectedTablesList, tablesResponse.getTables());
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(metadataStore, req, fileStore);
  }

  @Test
  public void testSample() throws IOException
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685

    IngestJobRequest ingestJobRequest = new IngestJobRequest(
        TEST_SCHEMA,
        null
    );

    String tableName = "testSample_table";
    String jobId = "testSample_jobId";

    SamplerResponse samplerResponse = new SamplerResponse(
        2,
        2,
        ImmutableList.of(
            new SamplerResponse.SamplerResponseRow(
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                false,
                null
            )
        )
    );

    EasyMock.expect(fileStore.makeInputSource(EasyMock.anyString()))
            .andReturn(new LocalInputSource(new File("a"), "a", null))
            .anyTimes();
    EasyMock.expect(overlordClient.sample(EasyMock.anyObject(SamplerSpec.class)))
            .andReturn(samplerResponse)
            .anyTimes();
    EasyMock.replay(fileStore, overlordClient, metadataStore, req);

    Response httpResponse = tablesResource.sampleIngestJob(
        ingestJobRequest,
        tableName,
        jobId
    );
    SamplerResponse samplerResponseFromAPICall = (SamplerResponse) httpResponse.getEntity();
    Assert.assertEquals(samplerResponse, samplerResponseFromAPICall);

    // make sure the first response has been cached successfully
    SamplerResponse cachedSamplerResponse = sampleStore.getSamplerResponse(jobId);
    Assert.assertEquals(samplerResponse, cachedSamplerResponse);

    // call the sample API again after first response has been cached
    httpResponse = tablesResource.sampleIngestJob(
        ingestJobRequest,
        tableName,
        jobId
    );
    samplerResponseFromAPICall = (SamplerResponse) httpResponse.getEntity();
    Assert.assertEquals(samplerResponse, samplerResponseFromAPICall);

    // delete the sample from cache
    sampleStore.deleteSample(jobId);
    Assert.assertNull(sampleStore.getSamplerResponse(jobId));

    // make sure sampling works again after deleting
    httpResponse = tablesResource.sampleIngestJob(
        ingestJobRequest,
        tableName,
        jobId
    );
    samplerResponseFromAPICall = (SamplerResponse) httpResponse.getEntity();
    Assert.assertEquals(samplerResponse, samplerResponseFromAPICall);

    EasyMock.verify(overlordClient);
  }

  private void expectAuthorizationTokenCheck()
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, null);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private static Set<TableInfo> getExpectedTables(Map<Table, Object2IntMap<JobState>> tableSummaries)
  {
    return tableSummaries.entrySet().stream().map(kvp -> {
      Table t = kvp.getKey();
      HashSet<Action> actions = new HashSet<>();
      if (TABLE_NAMES_WRITE.contains(t.getName())) {
        actions.add(Action.WRITE);
      } else if (TABLE_NAMES_READ.contains(t.getName())) {
        actions.add(Action.READ);
      }
      final List<TableInfo.JobStateCount> jobStateCounts;
      if (kvp.getValue() != null) {
        jobStateCounts = kvp.getValue()
                            .object2IntEntrySet()
                            .stream()
                            .map(e -> new TableInfo.JobStateCount(e.getKey(), e.getIntValue()))
                            .collect(Collectors.toList());
      } else {
        jobStateCounts = null;
      }
      return new TableInfo(
          t.getName(),
          t.getCreatedTime(),
          actions.size() > 0 ? actions : null,
          jobStateCounts
      );
    }).collect(Collectors.toSet());
  }
}
