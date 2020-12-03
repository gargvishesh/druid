/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.google.common.collect.ImmutableList;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class TablesResourceTest
{
  private static final String TABLE = "test";
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

  private FileStore fileStore;
  private IngestServiceMetadataStore metadataStore;
  private HttpServletRequest req;
  private TablesResource tablesResource;

  @Before
  public void setup()
  {
    fileStore = EasyMock.createMock(FileStore.class);
    metadataStore = EasyMock.createMock(IngestServiceMetadataStore.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            if (resource.getName().equals("allow")) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }

        };
      }
    };
    tablesResource = new TablesResource(tenantConfig, authMapper, fileStore, metadataStore);
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
    String id = UUIDUtils.generateUuid();
    EasyMock.expect(metadataStore.druidTableExists(EasyMock.eq(TABLE))).andReturn(false).once();

    EasyMock.replay(fileStore, metadataStore, req);

    Response response = tablesResource.stageIngestJob(null, TABLE);

    Map<String, Object> responseEntity = (Map<String, Object>) response.getEntity();

    Assert.assertEquals(404, response.getStatus());
  }


  @Test
  public void testScheduleIngestJobShouldSucceedWhenJobExists()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    // create fake schema for mocking:
    IngestSchema schema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null)
    );

    // Now we can mock the scheduleJob call:
    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(schema).anyTimes();
    EasyMock.expect(metadataStore.scheduleJob(id, schema)).andReturn(1).anyTimes();

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

    // create fake schema for mocking:
    IngestSchema schema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null)
    );

    // Now we can mock the scheduleJob call:
    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(schema).anyTimes();
    EasyMock.expect(metadataStore.scheduleJob(id, schema)).andReturn(0).anyTimes();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(404, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }

  @Test
  public void testScheduleIngestJobShouldFailWhenTooManyJobsForSameJobIdExist()
  {
    // expectAuthorizationTokenCheck(); see resource filter annotations are not called when testing in this manner
    // see https://github.com/apache/druid/issues/6685
    String id = UUIDUtils.generateUuid();

    // create fake schema for mocking:
    IngestSchema schema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null)
    );

    // Now we can mock the scheduleJob call:
    IngestJobRequest scheduleRequest = EasyMock.mock(IngestJobRequest.class);
    EasyMock.expect(scheduleRequest.getSchema()).andReturn(schema).anyTimes();
    EasyMock.expect(metadataStore.scheduleJob(id, schema)).andReturn(10).anyTimes();

    EasyMock.replay(fileStore, metadataStore, req);
    EasyMock.replay(scheduleRequest);

    Response response = tablesResource.scheduleIngestJob(scheduleRequest, TABLE, id);

    Assert.assertEquals(500, response.getStatus());
    EasyMock.verify(scheduleRequest);
  }
  @Test
  public void testInsertTable()
  {
    // this is the wrong check, unless we remain only using table write permission
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.insertTable(TABLE)).andReturn(1).once();
    EasyMock.replay(metadataStore, req, fileStore);
    Response response = tablesResource.createTable(TABLE, req);

    Map<String, Object> responseEntity = (Map<String, Object>) response.getEntity();
    // TODO: probably need to check some stuff in the response body here..

    Assert.assertEquals(200, response.getStatus());
  }

  private void expectAuthorizationTokenCheck()
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }
}
