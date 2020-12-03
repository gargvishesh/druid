/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobsResourceTest
{
  private static final String TABLE = "test";
  private static final String TABLE2 = "testNotAllowed";

  private static final List<IngestJob> JOB_LIST;
  private static final List<IngestJob> JOB_LIST_AUTHORIZED;
  private static final List<IngestJob> JOB_LIST_UNAUTHORIZED;

  static {
    JOB_LIST_AUTHORIZED = Arrays.asList(
        new IngestJob(TABLE, "job1", JobState.CANCELLED),
        new IngestJob(TABLE, "job2", JobState.COMPLETE),
        new IngestJob(TABLE, "job3", JobState.FAILED),
        new IngestJob(TABLE, "job4", JobState.RUNNING),
        new IngestJob(TABLE, "job5", JobState.SCHEDULED),
        new IngestJob(TABLE, "job6", JobState.STAGED)
    );

    JOB_LIST_UNAUTHORIZED = Arrays.asList(
        new IngestJob(TABLE2, "jobA", JobState.RUNNING),
        new IngestJob(TABLE2, "jobB", JobState.COMPLETE)
    );

    JOB_LIST = new ArrayList<>();
    JOB_LIST.addAll(JOB_LIST_AUTHORIZED);
    JOB_LIST.addAll(JOB_LIST_UNAUTHORIZED);
  }

  private IngestServiceMetadataStore metadataStore;
  private HttpServletRequest req;
  private JobsResource jobsResource;

  @Before
  public void setup()
  {
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
            if (!resource.getName().equals(TABLE2)) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }

        };
      }
    };
    jobsResource = new JobsResource(authMapper, metadataStore);
  }

  @After
  public void teardown()
  {
    EasyMock.verify(metadataStore, req);
  }

  @Test
  public void testGetAllJobs()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.getJobs(null)).andReturn(JOB_LIST).once();
    EasyMock.replay(metadataStore, req);

    Response response = jobsResource.getAllJobs(req, null);
    Map<String, List<IngestJob>> responseJobList = (Map<String, List<IngestJob>>) response.getEntity();
    Assert.assertEquals(JOB_LIST_AUTHORIZED, responseJobList.get("jobs"));
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetAllJobsSpecificState()
  {
    List<IngestJob> expectedJobList = Collections.singletonList(
        new IngestJob(TABLE, "job4", JobState.RUNNING)
    );

    List<IngestJob> metadataReturnList = JOB_LIST
        .stream()
        .filter(
            (job) -> job.getJobState() == JobState.RUNNING
        )
        .collect(Collectors.toList());

    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.getJobs(JobState.RUNNING))
            .andReturn(metadataReturnList)
            .once();
    EasyMock.replay(metadataStore, req);

    Response response = jobsResource.getAllJobs(req, JobState.RUNNING.toString());
    Map<String, List<IngestJob>> responseJobList = (Map<String, List<IngestJob>>) response.getEntity();
    Assert.assertEquals(expectedJobList, responseJobList.get("jobs"));
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetAllJobsInvalidStateFilter()
  {
    EasyMock.replay(metadataStore, req);
    Response response = jobsResource.getAllJobs(req, "invalidstate");
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetJobExists()
  {
    String id = "jobA";
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.getJob(id)).andReturn(new IngestJob(TABLE, id, JobState.COMPLETE)).anyTimes();
    EasyMock.replay(metadataStore, req);

    Response response = jobsResource.getJob(req, id);

    IngestJob ingestJob = (IngestJob) response.getEntity();
    Assert.assertEquals(id, ingestJob.getJobId());
    Assert.assertEquals(JobState.COMPLETE, ingestJob.getJobState());

    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetJobDoesNotExist()
  {
    String id = "jobA";
    EasyMock.expect(metadataStore.getJob(id)).andReturn(null).anyTimes();
    EasyMock.replay(metadataStore, req);

    Response response = jobsResource.getJob(req, id);

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetJobUnauthorized()
  {
    String id = "jobA";
    expectAuthorizationTokenCheck();
    EasyMock.expect(metadataStore.getJob(id)).andReturn(new IngestJob(TABLE2, id, JobState.COMPLETE)).anyTimes();
    EasyMock.replay(metadataStore, req);

    Response response = jobsResource.getJob(req, id);

    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
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
