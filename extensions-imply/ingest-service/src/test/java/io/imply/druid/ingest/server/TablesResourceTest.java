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
import io.imply.druid.ingest.metadata.TableJobStateStats;
import io.imply.druid.ingest.samples.SampleStore;
import io.imply.druid.ingest.samples.local.LocalSampleStore;
import io.imply.druid.ingest.samples.local.LocalSampleStoreConfig;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
  private final IngestServiceTenantConfig tenantConfig = new IngestServiceTenantConfig(
      "test-account",
      "test-cluster-1"
  );

  private FileStore fileStore;
  private IngestServiceMetadataStore metadataStore;
  private HttpServletRequest req;
  private TablesResource tablesResource;
  private SampleStore sampleStore;
  private OverlordClient overlordClient;
  private ObjectMapper jsonMapper;

  private static final List<Table> TABLE_LIST;
  private static final List<Table> TABLE_LIST_WRITE;
  private static final List<Table> TABLE_LIST_READ;
  private static final List<Table> TABLE_LIST_UNAUTHORIZED;
  private static final List<String> TABLE_NAMES_WRITE;
  private static final List<String> TABLE_NAMES_READ;
  private static final List<String> TABLE_NAMES_UNAUTHORIZED;

  static {

    TABLE_LIST_WRITE = Arrays.asList(
        new Table("foo1", DateTimes.nowUtc()),
        new Table("foo2", DateTimes.nowUtc()),
        new Table("foo3", DateTimes.nowUtc()),
        new Table("foo4", DateTimes.nowUtc()),
        new Table("foo5", DateTimes.nowUtc()),
        new Table("foo6", DateTimes.nowUtc())
    );

    TABLE_LIST_READ = Arrays.asList(
        new Table("bar1", DateTimes.nowUtc()),
        new Table("bar2", DateTimes.nowUtc()),
        new Table("bar3", DateTimes.nowUtc())
    );

    TABLE_LIST_UNAUTHORIZED = Arrays.asList(
        new Table("crook1", DateTimes.nowUtc()),
        new Table("crook2", DateTimes.nowUtc()),
        new Table("crook3", DateTimes.nowUtc())
    );

    TABLE_LIST = new ArrayList<>();
    TABLE_LIST.addAll(TABLE_LIST_WRITE);
    TABLE_LIST.addAll(TABLE_LIST_READ);
    TABLE_LIST.addAll(TABLE_LIST_UNAUTHORIZED);

    TABLE_NAMES_WRITE = TABLE_LIST_WRITE.stream().map(Table::getName).collect(Collectors.toList());
    TABLE_NAMES_READ = TABLE_LIST_READ.stream().map(Table::getName).collect(Collectors.toList());
    TABLE_NAMES_UNAUTHORIZED = TABLE_LIST_UNAUTHORIZED.stream().map(Table::getName).collect(Collectors.toList());
  }

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

    AtomicInteger i = new AtomicInteger(100);
    Set<TableJobStateStats> expectedTablesSet = TABLE_LIST.stream().map(t -> {
      TableJobStateStats ts = new TableJobStateStats(t);
      if (TABLE_NAMES_WRITE.contains(t.getName())) {
        ts.addPermissions(Action.WRITE);
      } else if (TABLE_NAMES_READ.contains(t.getName())) {
        ts.addPermissions(Action.READ);
      }
      return ts;
    }).collect(Collectors.toSet());
    List<TableJobStateStats> expectedTablesList = new ArrayList<>(expectedTablesSet);
    expectedTablesList.sort(Comparator.comparing(TableJobStateStats::getName));

    EasyMock.expect(metadataStore.getJobCountPerTablePerState(Collections.emptySet()))
            .andReturn(expectedTablesSet);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("NONE", req);
    Map<String, List<TableJobStateStats>> responseTableStatsMap = (Map<String, List<TableJobStateStats>>) response.getEntity();

    Assert.assertEquals(responseTableStatsMap.get("tables"), expectedTablesList);

    Assert.assertEquals(200, response.getStatus());
  }


  @Test
  public void testTablesGetShouldReturnOnlyTablesWithRunningState()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Set<TableJobStateStats> expectedTablesSet =
        TABLE_LIST
            .stream()
            .filter(t -> t.getName().equals(TABLE_NAMES_WRITE.get(0))
                         || t.getName().equals(TABLE_NAMES_WRITE.get(1))
                         || t.getName().equals(TABLE_NAMES_READ.get(2)))
            .map(t -> {
              TableJobStateStats ts = new TableJobStateStats(t);
              if (TABLE_NAMES_WRITE.contains(t.getName())) {
                ts.addPermissions(Action.WRITE);
              } else if (TABLE_NAMES_READ.contains(t.getName())) {
                ts.addPermissions(Action.READ);
              }
              ts.addJobState(JobState.RUNNING, i.addAndGet(1));
              return ts;
            }).collect(Collectors.toSet());
    List<TableJobStateStats> expectedTablesList = new ArrayList<>(expectedTablesSet);
    expectedTablesList.sort(Comparator.comparing(TableJobStateStats::getName));

    EasyMock.expect(metadataStore.getJobCountPerTablePerState(Collections.singleton(JobState.RUNNING)))
            .andReturn(expectedTablesSet);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("RUNNING", req);
    Map<String, List<TableJobStateStats>> responseTableStatsMap = (Map<String, List<TableJobStateStats>>) response.getEntity();

    Assert.assertEquals(responseTableStatsMap.get("tables"), expectedTablesList);

    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testTablesGetShouldReturnAllTablesWithOrWithoutJobs()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Set<TableJobStateStats> expectedTablesSet =
        TABLE_LIST
            .stream()
            .map(t -> {
              TableJobStateStats ts = new TableJobStateStats(t);
              if (TABLE_NAMES_WRITE.contains(t.getName())) {
                ts.addPermissions(Action.WRITE);
              } else {
                ts.addPermissions(Action.READ);
              }
              return ts;
            })
            .map(t -> {
              TableJobStateStats ts = new TableJobStateStats(t);
              if (TABLE_NAMES_WRITE.get(0).equals(t.getName())) {
                ts.addJobState(JobState.STAGED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(1).equals(t.getName())) {
                ts.addJobState(JobState.RUNNING, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(2).equals(t.getName())) {
                ts.addJobState(JobState.SCHEDULED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(3).equals(t.getName())) {
                ts.addJobState(JobState.CANCELLED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(4).equals(t.getName())) {
                ts.addJobState(JobState.COMPLETE, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(5).equals(t.getName())) {
                ts.addJobState(JobState.FAILED, i.addAndGet(1));
              }
              return ts;
            }).collect(Collectors.toSet());
    List<TableJobStateStats> expectedTablesList = new ArrayList<>(expectedTablesSet);
    expectedTablesList.sort(Comparator.comparing(TableJobStateStats::getName));

    EasyMock.expect(metadataStore.getJobCountPerTablePerState(Collections.singleton(JobState.RUNNING)))
            .andReturn(expectedTablesSet);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("RUNNING", req);
    Map<String, List<TableJobStateStats>> responseTableStatsMap = (Map<String, List<TableJobStateStats>>) response.getEntity();

    Assert.assertEquals(responseTableStatsMap.get("tables"), expectedTablesList);

    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testTablesGetShouldNotReturnUnauthorizedTables()
  {
    expectAuthorizationTokenCheck();

    AtomicInteger i = new AtomicInteger(100);
    Set<TableJobStateStats> expectedTablesSet =
        TABLE_LIST
            .stream()
            .map(t -> {
              TableJobStateStats ts = new TableJobStateStats(t);
              if (TABLE_NAMES_WRITE.contains(t.getName())) {
                ts.addPermissions(Action.WRITE);
              } else {
                ts.addPermissions(Action.READ);
              }
              return ts;
            })
            .map(t -> {
              TableJobStateStats ts = new TableJobStateStats(t);
              if (TABLE_NAMES_WRITE.get(0).equals(t.getName())) {
                ts.addJobState(JobState.STAGED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(1).equals(t.getName())) {
                ts.addJobState(JobState.RUNNING, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(2).equals(t.getName())) {
                ts.addJobState(JobState.SCHEDULED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(3).equals(t.getName())) {
                ts.addJobState(JobState.CANCELLED, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(4).equals(t.getName())) {
                ts.addJobState(JobState.COMPLETE, i.addAndGet(1));
              } else if (TABLE_NAMES_WRITE.get(5).equals(t.getName())) {
                ts.addJobState(JobState.FAILED, i.addAndGet(1));
              }
              return ts;
            })
            .filter(ts -> !TABLE_NAMES_UNAUTHORIZED.contains(ts.getName()))
            .collect(Collectors.toSet());
    List<TableJobStateStats> expectedTablesList = new ArrayList<>(expectedTablesSet);
    expectedTablesList.sort(Comparator.comparing(TableJobStateStats::getName));

    EasyMock.expect(metadataStore.getJobCountPerTablePerState(null))
            .andReturn(expectedTablesSet);
    EasyMock.replay(metadataStore, req, fileStore);

    Response response = tablesResource.getTables("ALL", req);
    Map<String, List<TableJobStateStats>> responseTableStatsMap = (Map<String, List<TableJobStateStats>>) response.getEntity();

    Assert.assertEquals(responseTableStatsMap.get("tables"), expectedTablesList);

    Assert.assertEquals(200, response.getStatus());
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
}
