/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.sql.calcite.view.state.manager.ViewStateManagerResource.ViewDefinitionRequest;
import io.imply.druid.tests.query.ITViewManagerAndQueryTest;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.https.SSLClientConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.clients.SqlResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Test(groups = TestNGGroup.LDAP_SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITViewTestWithJdbcAndLdap
{
  private static final String TEST_DATA_SOURCE = "auth_test";
  private static final String TEST_VIEW = "view_test";
  private static final String TEST_VIEW2 = TEST_DATA_SOURCE; // the same name as the datasource
  private static final String CONNECTION_TEMPLATE = "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/";
  private static final String TLS_CONNECTION_TEMPLATE =
      "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/;truststore=%s;truststore_password=%s;keystore=%s;keystore_password=%s;key_password=%s";

  private static final String QUERY_TEMPLATE =
      "SELECT \"user\", SUM(\"added\"), COUNT(*) " +
      "FROM \"%s\".\"%s\" " +
      "WHERE \"language\" = %s " +
      "GROUP BY 1 ORDER BY 3 DESC LIMIT 10";
  private static final String DATASOURCE_QUERY = StringUtils.format(QUERY_TEMPLATE, "druid", TEST_DATA_SOURCE, "'en'");
  private static final String VIEW_QUERY = StringUtils.format(QUERY_TEMPLATE, "view", TEST_VIEW, "'en'");
  private static final String VIEW_QUERY2 = StringUtils.format(QUERY_TEMPLATE, "view", TEST_VIEW2, "'en'");

  private static final String VIEW_SPEC = StringUtils.format(
      "SELECT * FROM druid.%s WHERE \"language\" = 'en'",
      TEST_DATA_SOURCE
  );

  private static final String DATASOURCE_QUERY_PARAM = StringUtils.format(QUERY_TEMPLATE, "druid", TEST_DATA_SOURCE, "?");
  private static final String VIEW_QUERY_PARAM = StringUtils.format(QUERY_TEMPLATE, "view", TEST_VIEW, "?");

  private static final String VIEW_METADATA_QUERIES_RESOURCE = "/queries/view_metadata_queries.json";

  @Inject
  protected ObjectMapper jsonMapper;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  SSLClientConfig sslConfig;

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;

  @Inject
  @Client
  private HttpClient httpClient;

  private String[] connections;
  private HttpClient adminClient;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );
    setupUsers();
    // ensure that wikipedia segments are loaded completely
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded(TEST_DATA_SOURCE), "wikipedia segment load"
    );

    connections = new String[]{
        StringUtils.format(CONNECTION_TEMPLATE, config.getRouterUrl()),
        StringUtils.format(CONNECTION_TEMPLATE, config.getBrokerUrl()),
        StringUtils.format(
            TLS_CONNECTION_TEMPLATE,
            config.getRouterTLSUrl(),
            sslConfig.getTrustStorePath(),
            sslConfig.getTrustStorePasswordProvider().getPassword(),
            sslConfig.getKeyStorePath(),
            sslConfig.getKeyStorePasswordProvider().getPassword(),
            sslConfig.getKeyManagerPasswordProvider().getPassword()
        ),
        StringUtils.format(
            TLS_CONNECTION_TEMPLATE,
            config.getBrokerTLSUrl(),
            sslConfig.getTrustStorePath(),
            sslConfig.getTrustStorePasswordProvider().getPassword(),
            sslConfig.getKeyStorePath(),
            sslConfig.getKeyStorePasswordProvider().getPassword(),
            sslConfig.getKeyManagerPasswordProvider().getPassword()
        )
    };

    createView(TEST_VIEW, new ViewDefinitionRequest(VIEW_SPEC));
    createView(TEST_VIEW2, new ViewDefinitionRequest(VIEW_SPEC));
    final HttpClient viewOnlyUserClient = new CredentialedHttpClient(
        new BasicCredentials("viewOnlyUser", "helloworld"),
        httpClient
    );
    final SqlTestQueryHelper queryHelper = new SqlTestQueryHelper(
        jsonMapper,
        new SqlResourceTestClient(jsonMapper, viewOnlyUserClient, config),
        config
    );
    waitForViewsToLoad(queryHelper);
  }

  @Test
  public void testStatementDatasourceOnlyUserQueryDatasourceSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("datasourceOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          try (ResultSet resultSet = statement.executeQuery(DATASOURCE_QUERY)) {
            int resultRowCount = 0;
            while (resultSet.next()) {
              resultRowCount++;
            }
            Assert.assertEquals(resultRowCount, 10);
          }
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementDatasourceOnlyUserQueryViewFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("datasourceOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementDatasourceOnlyUserQueryView2Fail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("datasourceOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY2);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testPreparedStatementDatasourceOnlyUserQueryDatasourceSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("datasourceOnlyUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(DATASOURCE_QUERY_PARAM)) {
          statement.setString(1, "en");
          try (ResultSet resultSet = statement.executeQuery()) {
            int resultRowCount = 0;
            while (resultSet.next()) {
              resultRowCount++;
            }
            Assert.assertEquals(resultRowCount, 10);
          }
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class)
  public void testPreparedStatementDatasourceOnlyUserQueryViewFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("datasourceOnlyUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(VIEW_QUERY_PARAM)) {
          statement.setString(1, "en");
          try (ResultSet resultSet = statement.executeQuery()) {
            int resultRowCount = 0;
            while (resultSet.next()) {
              resultRowCount++;
            }
            Assert.assertEquals(resultRowCount, 10);
          }
        }
      }
    }
  }

  @Test
  public void testStatementViewOnlyUserQueryViewSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testStatementViewOnlyUserQueryView2Success() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY2);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementViewOnlyUserQueryDatasourceFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewOnlyUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(DATASOURCE_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testPreparedStatementViewOnlyUserQueryViewSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewOnlyUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(VIEW_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class)
  public void testPreparedStatementViewOnlyUserQueryDatasourceFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewOnlyUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(DATASOURCE_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testStatementViewAndDatasourceUserQueryViewSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testStatementViewAndDatasourceUserQueryView2Success() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY2);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testStatementViewAndDatasourceUserQueryDatasourceSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(DATASOURCE_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testPreparedStatementViewAndDatasourceUserQueryViewSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewAndDatasourceUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(VIEW_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testPreparedStatementViewAndDatasourceUserQueryDatasourceSuccess() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("viewAndDatasourceUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(DATASOURCE_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementNoViewAndDatasourceUserQueryViewFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("noViewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementNoViewAndDatasourceUserQueryView2Fail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("noViewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(VIEW_QUERY2);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".*-> ForbiddenException: Allowed:false, Message:")
  public void testStatementNoViewAndDatasourceUserQueryDatasourceFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("noViewAndDatasourceUser"))) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(DATASOURCE_QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class)
  public void testPreparedStatementNoViewAndDatasourceUserQueryViewFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("noViewAndDatasourceUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(VIEW_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class)
  public void testPreparedStatementNoViewAndDatasourceUserQueryDatasourceFail() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, createProperties("noViewAndDatasourceUser"))) {
        try (PreparedStatement statement = connection.prepareStatement(DATASOURCE_QUERY_PARAM)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
    }
  }

  protected void setupUsers() throws Exception
  {
    // create a role that can only read the datasource
    List<ResourceAction> readDatasourceOnlyPermissions = Collections.singletonList(
        new ResourceAction(
            new Resource(TEST_DATA_SOURCE, ResourceType.DATASOURCE),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "datasourceOnlyGroup",
        ImmutableMap.of("datasourceOnlyRole", readDatasourceOnlyPermissions)
    );

    // create a role that can only read the views
    List<ResourceAction> readViewOnlyPermissions = ImmutableList.of(
        new ResourceAction(
            new Resource(TEST_VIEW, ResourceType.VIEW),
            Action.READ
        ),
        new ResourceAction(
            new Resource(TEST_VIEW2, ResourceType.VIEW),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "viewOnlyGroup",
        ImmutableMap.of("viewOnlyRole", readViewOnlyPermissions)
    );

    // create a role that can read both the views and datasource
    List<ResourceAction> readViewAndDatasourcePermissions = ImmutableList.of(
        new ResourceAction(
            new Resource(TEST_DATA_SOURCE, ResourceType.DATASOURCE),
            Action.READ
        ),
        new ResourceAction(
            new Resource(TEST_VIEW, ResourceType.VIEW),
            Action.READ
        ),
        new ResourceAction(
            new Resource(TEST_VIEW2, ResourceType.VIEW),
            Action.READ
        )
    );

    createRoleWithPermissionsAndGroupMapping(
        "viewAndDatasourceGroup",
        ImmutableMap.of("viewAndDatasourceRole", readViewAndDatasourcePermissions)
    );

    // create a role that cannot read the views or datasource
    List<ResourceAction> noReadPermissions = ImmutableList.of();

    createRoleWithPermissionsAndGroupMapping(
        "noViewAndDatasourceGroup",
        ImmutableMap.of("noViewAndDatasourceRole", noReadPermissions)
    );
  }

  private void createRoleWithPermissionsAndGroupMapping(
      String group,
      Map<String, List<ResourceAction>> roleTopermissions
  ) throws Exception
  {
    roleTopermissions.keySet().forEach(role -> HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null
    ));

    for (Map.Entry<String, List<ResourceAction>> entry : roleTopermissions.entrySet()) {
      String role = entry.getKey();
      List<ResourceAction> permissions = entry.getValue();
      byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          StringUtils.format(
              "%s/druid-ext/basic-security/authorization/db/ldapauth/roles/%s/permissions",
              config.getCoordinatorUrl(),
              role
          ),
          permissionsBytes
      );
    }

    String groupMappingName = StringUtils.format("%sMapping", group);
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping(
        groupMappingName,
        StringUtils.format("cn=%s,ou=Groups,dc=example,dc=org", group),
        roleTopermissions.keySet()
    );
    byte[] groupMappingBytes = jsonMapper.writeValueAsBytes(groupMapping);
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/ldapauth/groupMappings/%s",
            config.getCoordinatorUrl(),
            groupMappingName
        ),
        groupMappingBytes
    );
  }

  private void createView(String viewName, ViewDefinitionRequest viewSpec) throws Exception
  {
    String url = StringUtils.format("%s/druid-ext/view-manager/v1/views/%s", config.getCoordinatorUrl(), viewName);
    InputStreamFullResponseHolder response = adminClient
        .go(
            new Request(HttpMethod.POST, new URL(url)).setContent(
                "application/json",
                jsonMapper.writeValueAsBytes(viewSpec)
            ),
            new InputStreamFullResponseHandler()
        )
        .get();

    if (!response.getStatus().equals(HttpResponseStatus.CREATED)) {
      throw new ISE(
          "Error while creating a view[%s] with spec[%s] status[%s] content[%s]",
          viewName,
          viewSpec,
          response.getStatus(),
          jsonMapper.readValue(response.getContent(), new TypeReference<Map<String, Object>>()
          {
          })
      );
    }
  }

  private void waitForViewsToLoad(SqlTestQueryHelper queryHelper)
  {
    // wait until views are available in metadata queries
    ITRetryUtil.retryUntilTrue(
        () -> {
          try {
            queryHelper.testQueriesFromString(
                queryHelper.getQueryURL(config.getRouterUrl()),
                ITViewManagerAndQueryTest.replaceViewTemplate(
                    AbstractIndexerTest.getResourceAsString(VIEW_METADATA_QUERIES_RESOURCE),
                    TEST_VIEW
                )
            );
            return true;
          }
          catch (Exception ex) {
            return false;
          }
        },
        StringUtils.format("waiting for %s to be loaded", TEST_VIEW)
    );

    ITRetryUtil.retryUntilTrue(
        () -> {
          try {
            queryHelper.testQueriesFromString(
                queryHelper.getQueryURL(config.getRouterUrl()),
                ITViewManagerAndQueryTest.replaceViewTemplate(
                    AbstractIndexerTest.getResourceAsString(VIEW_METADATA_QUERIES_RESOURCE),
                    TEST_VIEW2
                )
            );
            return true;
          }
          catch (Exception ex) {
            return false;
          }
        },
        StringUtils.format("waiting for %s to be loaded", TEST_VIEW2)
    );
  }

  private static Properties createProperties(String user)
  {
    final Properties properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", "helloworld");
    return properties;
  }
}
