/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource;
import org.apache.druid.curator.discovery.ServerDiscoveryFactory;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Test(groups = TestNGGroup.QUERY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITViewManagerAndQueryTest
{
  private static final Logger LOG = new Logger(ITViewManagerAndQueryTest.class);
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  private static final String WIKI_EN_VIEW = "wiki_en";
  private static final String VIEW_METADATA_QUERIES_RESOURCE = "/queries/view_metadata_queries.json";
  private static final String VIEW_QUERIES_RESOURCE = "/queries/view_queries.json";

  final String BASE_VIEW_MANAGER_PATH = "druid-ext/view-manager/v1/views";

  @Inject
  ObjectMapper objectMapper;

  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @BeforeMethod
  public void setup() throws Exception
  {
    // ensure that wikipedia segments are loaded completely
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded(WIKIPEDIA_DATA_SOURCE), "wikipedia segment load"
    );

    // admin user needs view read permissions.. grant everything
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(
            new Resource(".*", ResourceType.DATASOURCE),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.DATASOURCE),
            Action.WRITE
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.VIEW),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.VIEW),
            Action.WRITE
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.CONFIG),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.CONFIG),
            Action.WRITE
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.WRITE
        )
    );

    byte[] permissionsBytes = objectMapper.writeValueAsBytes(permissions);
    HttpUtil.makeRequest(
        httpClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s/permissions",
            config.getCoordinatorUrl(),
            "admin"
        ),
        permissionsBytes
    );
  }

  @Test
  public void testHappyPath() throws Exception
  {
    Map<String, ImplyViewDefinition> views = getViews();

    Assert.assertEquals(views.size(), 0);

    ImplyViewDefinition newViewWhoDis = new ImplyViewDefinition(
        WIKI_EN_VIEW,
        StringUtils.format("SELECT * FROM \"%s\" WHERE \"language\" = 'en'", WIKIPEDIA_DATA_SOURCE)
    );
    createView(newViewWhoDis);

    views = getViews();
    Assert.assertEquals(views.size(), 1);
    Assert.assertEquals(newViewWhoDis.getViewName(), views.get(WIKI_EN_VIEW).getViewName());
    Assert.assertEquals(newViewWhoDis.getViewSql(), views.get(WIKI_EN_VIEW).getViewSql());

    ImplyViewDefinition altered = newViewWhoDis.withSql(
        StringUtils.format(
            "SELECT * FROM \"%s\" WHERE \"language\" = 'en' AND \"namespace\" = 'article'",
            WIKIPEDIA_DATA_SOURCE
        )
    );

    alterView(altered);

    views = getViews();
    Assert.assertEquals(views.size(), 1);
    Assert.assertEquals(altered.getViewName(), views.get(WIKI_EN_VIEW).getViewName());
    Assert.assertEquals(altered.getViewSql(), views.get(WIKI_EN_VIEW).getViewSql());

    // wait until views are available in metadata queries
    ITRetryUtil.retryUntilTrue(
        () -> {
          try {
            queryHelper.testQueriesFromString(
                queryHelper.getQueryURL(config.getRouterUrl()),
                replaceViewTemplate(
                    AbstractIndexerTest.getResourceAsString(VIEW_METADATA_QUERIES_RESOURCE),
                    WIKI_EN_VIEW
                )
            );
            return true;
          }
          catch (Exception ex) {
            return false;
          }
        },
        "waiting for SQL metadata refresh"
    );

    // now do some queries
    queryHelper.testQueriesFromString(
        queryHelper.getQueryURL(config.getRouterUrl()),
        replaceViewTemplate(AbstractIndexerTest.getResourceAsString(VIEW_QUERIES_RESOURCE), WIKI_EN_VIEW)
    );

    deleteView(altered);
    views = getViews();
    Assert.assertEquals(views.size(), 0);
  }

  @Test
  public void testMultipleCreateAndLoadStatus() throws Exception
  {
    for (int i = 0; i < 10; i++) {
      String viewName = StringUtils.format("loadStatusTest%s", i);
      ImplyViewDefinition viewDefinition = new ImplyViewDefinition(
          viewName,
          StringUtils.format(
              "SELECT * FROM \"%s\" WHERE \"language\" = 'en' AND \"namespace\" = 'article'",
              WIKIPEDIA_DATA_SOURCE
          )
      );
      createView(viewDefinition);
    }

    for (int i = 0; i < 10; i++) {
      String viewName = StringUtils.format("loadStatusTest%s", i);
      ITRetryUtil.retryUntilTrue(
          () -> {
            try {
              ViewStateManagerResource.ViewLoadStatusResponse loadStatus = getViewLoadStatus(viewName);
              int numBrokers = 1;
              if (loadStatus.getFresh().size() == numBrokers) {
                // there shouldn't be any other brokers
                Assert.assertEquals(loadStatus.getStale().size(), 0);
                Assert.assertEquals(loadStatus.getUnknown().size(), 0);
                return true;
              }
              return false;
            }
            catch (Exception ex) {
              return false;
            }
          },
          "waiting for view " + viewName + " to be loaded"
      );
    }

    for (int i = 0; i < 10; i++) {
      String viewName = StringUtils.format("loadStatusTest%s", i);

      // now do some queries
      queryHelper.testQueriesFromString(
          queryHelper.getQueryURL(config.getRouterUrl()),
          replaceViewTemplate(AbstractIndexerTest.getResourceAsString(VIEW_QUERIES_RESOURCE), viewName)
      );
    }
  }

  private Map<String, ImplyViewDefinition> getViews() throws IOException
  {
    return getRequest(
        config.getCoordinatorUrl(),
        BASE_VIEW_MANAGER_PATH,
        new TypeReference<Map<String, ImplyViewDefinition>>() { }
    );
  }

  private ViewStateManagerResource.ViewLoadStatusResponse getViewLoadStatus(String viewName) throws IOException
  {
    return getRequest(
        config.getCoordinatorUrl(),
        StringUtils.format("%s/loadstatus/%s", BASE_VIEW_MANAGER_PATH, viewName),
        ViewStateManagerResource.ViewLoadStatusResponse.class
    );
  }

  private void createView(ImplyViewDefinition def) throws MalformedURLException, JsonProcessingException
  {
    URL url = makeUrl(config.getCoordinatorUrl(), BASE_VIEW_MANAGER_PATH, def.getViewName());
    InputStreamFullResponseHolder holder = doRequest(makeViewRequest(HttpMethod.POST, url, def));
    Assert.assertEquals(holder.getStatus(), HttpResponseStatus.CREATED);
  }

  private void alterView(ImplyViewDefinition def) throws MalformedURLException, JsonProcessingException
  {
    URL url = makeUrl(config.getCoordinatorUrl(), BASE_VIEW_MANAGER_PATH, def.getViewName());
    InputStreamFullResponseHolder holder = doRequest(makeViewRequest(HttpMethod.PUT, url, def));
    Assert.assertEquals(holder.getStatus(), HttpResponseStatus.OK);
  }

  private Request makeViewRequest(HttpMethod method, URL url, ImplyViewDefinition def) throws JsonProcessingException
  {
    return new Request(method, url).addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON)
                                   .addHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                                   .setContent(objectMapper.writeValueAsBytes(def));
  }

  private void deleteView(ImplyViewDefinition def) throws MalformedURLException
  {
    InputStreamFullResponseHolder holder = doRequest(
        new Request(HttpMethod.DELETE, makeUrl(config.getCoordinatorUrl(), BASE_VIEW_MANAGER_PATH, def.getViewName()))
    );
    Assert.assertEquals(holder.getStatus(), HttpResponseStatus.OK);
  }

  private URL makeUrl(String... args) throws MalformedURLException
  {
    return new URL(String.join("/", args));
  }

  private <T> T getRequest(String host, String path, Class<T> clazz) throws IOException
  {
    URL url = new URL(
        StringUtils.format(
            "%s/%s",
            host,
            path
        )
    );

    return doRequest(new Request(HttpMethod.GET, url), clazz);
  }

  private <T> T getRequest(String host, String path, TypeReference<T> typeReference) throws IOException
  {
    URL url = new URL(
        StringUtils.format(
            "%s/%s",
            host,
            path
        )
    );

    return doRequest(new Request(HttpMethod.GET, url), typeReference);
  }

  private <T> T doRequest(Request request, TypeReference<T> typeReference) throws IOException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
      throw new RE(
          "Failed to talk to node at [%s]. Error code[%d], description[%s].",
          request.getUrl(),
          responseHolder.getStatus().getCode(),
          responseHolder.getStatus().getReasonPhrase()
      );
    }
    return objectMapper.readValue(responseHolder.getContent(), typeReference);
  }

  private <T> T doRequest(Request request, Class<T> clazz) throws IOException
  {
    InputStreamFullResponseHolder responseHolder = doRequest(request);
    if (responseHolder.getStatus().getCode() != HttpServletResponse.SC_OK) {
      throw new RE(
          "Failed to talk to node at [%s]. Error code[%d], description[%s].",
          request.getUrl(),
          responseHolder.getStatus().getCode(),
          responseHolder.getStatus().getReasonPhrase()
      );
    }
    return objectMapper.readValue(responseHolder.getContent(), clazz);
  }


  private InputStreamFullResponseHolder doRequest(Request request)
  {
    InputStreamFullResponseHolder responseHolder;
    try {
      responseHolder = httpClient.go(
          request,
          new InputStreamFullResponseHandler()
      ).get();
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return responseHolder;
  }

  public static String replaceViewTemplate(String template, String view)
  {
    return StringUtils.replace(
        template,
        "%%VIEW%%",
        view
    );
  }
}
