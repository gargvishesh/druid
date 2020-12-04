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
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.sql.IngestServiceSqlMetadataStore;
import io.imply.druid.ingest.metadata.sql.IngestServiceSqlMetatadataConfig;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
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
import org.junit.Rule;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

public class SchemasResourceTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private final IngestServiceSqlMetatadataConfig config = IngestServiceSqlMetatadataConfig.DEFAULT_CONFIG;

  private SQLMetadataConnector connector;
  private HttpServletRequest req;
  private SchemasResource schemasResource;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setup()
  {
    connector = derbyConnectorRule.getConnector();
    IngestServiceSqlMetadataStore metadataStore = new IngestServiceSqlMetadataStore(() -> config, connector, MAPPER);
    connector.retryWithHandle((handle) -> connector.tableExists(handle, config.getSchemasTable()));

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
            return Access.OK;
          }
        };
      }
    };
    schemasResource = new SchemasResource(authMapper, metadataStore, MAPPER);
  }

  @After
  public void teardown()
  {
    EasyMock.verify(req);
  }

  @Test
  public void testCreateAndGetSchema() throws Exception
  {
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    EasyMock.replay(req);

    Response response = schemasResource.createSchema(
        new ByteArrayInputStream(MAPPER.writeValueAsBytes(someSchema)),
        req
    );
    Map<String, Integer> createSchemaResponse = (Map<String, Integer>) response.getEntity();
    Assert.assertEquals(new Integer(1), createSchemaResponse.get("schemaId"));
    Assert.assertEquals(200, response.getStatus());

    response = schemasResource.getSchema("1", req);
    IngestSchema getSchemaResponse = (IngestSchema) response.getEntity();
    Assert.assertEquals(someSchema, getSchemaResponse);
  }

  @Test
  public void testCreateSchemaInvalidRequestJson() throws Exception
  {
    EasyMock.replay(req);

    Response response = schemasResource.createSchema(
        new ByteArrayInputStream(MAPPER.writeValueAsBytes("[[}[$$$garbagestring")),
        req
    );
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testListSchemas() throws Exception
  {
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    IngestSchema someSchema2 = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("a"),
                StringDimensionSchema.create("b")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema 2"
    );

    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    EasyMock.replay(req);

    // make 2 schemas
    Response response = schemasResource.createSchema(
        new ByteArrayInputStream(MAPPER.writeValueAsBytes(someSchema)),
        req
    );
    Map<String, Integer> createSchemaResponse = (Map<String, Integer>) response.getEntity();
    Assert.assertEquals(new Integer(1), createSchemaResponse.get("schemaId"));
    Assert.assertEquals(200, response.getStatus());

    Response response2 = schemasResource.createSchema(
        new ByteArrayInputStream(MAPPER.writeValueAsBytes(someSchema2)),
        req
    );
    Map<String, Integer> createSchemaResponse2 = (Map<String, Integer>) response2.getEntity();
    Assert.assertEquals(new Integer(2), createSchemaResponse2.get("schemaId"));
    Assert.assertEquals(200, response2.getStatus());

    // check that both schemas are in the list
    Response getAllSchemasResponse = schemasResource.getAllSchemas(req);
    Map<String, List<IngestSchema>> allSchemas = (Map<String, List<IngestSchema>>) getAllSchemasResponse.getEntity();
    Assert.assertEquals(ImmutableList.of(someSchema, someSchema2), allSchemas.get("schemas"));
  }

  @Test
  public void testDeleteSchema() throws Exception
  {
    IngestSchema someSchema = new IngestSchema(
        new TimestampSpec("time", "iso", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("x"),
                StringDimensionSchema.create("y")
            )
        ),
        new JsonInputFormat(null, null, null),
        "test schema"
    );

    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    expectAuthorizationTokenCheck();
    EasyMock.replay(req);

    Response response = schemasResource.createSchema(
        new ByteArrayInputStream(MAPPER.writeValueAsBytes(someSchema)),
        req
    );
    Map<String, Integer> createSchemaResponse = (Map<String, Integer>) response.getEntity();
    Assert.assertEquals(new Integer(1), createSchemaResponse.get("schemaId"));
    Assert.assertEquals(200, response.getStatus());

    response = schemasResource.getSchema("1", req);
    IngestSchema getSchemaResponse = (IngestSchema) response.getEntity();
    Assert.assertEquals(someSchema, getSchemaResponse);

    // Delete and check that the schema is gone
    response = schemasResource.deleteSchema("1", req);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    response = schemasResource.getSchema("1", req);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

    // Non-existent schemaId
    response = schemasResource.deleteSchema("999", req);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

    // Invalid schemaId format
    response = schemasResource.deleteSchema("notanumber", req);
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
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
