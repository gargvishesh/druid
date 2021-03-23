/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.KeycloakedHttpClient;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleSimplifiedPermissions;
import io.imply.druid.tests.ImplyTestNGGroup;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.security.AbstractAuthConfigurationTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.keycloak.OAuth2Constants;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.authentication.ClientCredentialsProviderUtils;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// TODO: renable with IMPLY-6305
@Test(groups = ImplyTestNGGroup.KEYCLOAK_SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKeycloakAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITKeycloakAuthConfigurationTest.class);

  static final TypeReference<Set<String>> ROLES_RESULTS_TYPE_REFERENCE =
      new TypeReference<Set<String>>()
      {
      };

  static final TypeReference<List<KeycloakAuthorizerPermission>> ROLE_PERMISSIONS_RESULT_TYPE_REFERENCE =
      new TypeReference<List<KeycloakAuthorizerPermission>>()
      {
      };

  private static final String KEYCLOAK_AUTHENTICATOR = "ImplyKeycloakAuthenticator";
  private static final String KEYCLOAK_AUTHORIZER = "ImplyKeycloakAuthorizer";

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  private HttpClient druidUserNotExistClient;

  @BeforeClass
  public void before() throws Exception
  {
    // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
    );

    setupHttpClients();
    setupUsers();
    setExpectedSystemSchemaObjects();
  }

  @Test
  public void test_systemSchemaAccess_admin() throws Exception
  {
    // check that admin access works on all nodes
    checkNodeAccess(adminClient);

    // as admin
    LOG.info("Checking sys.segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    LOG.info("Checking sys.servers query as admin...");
    verifySystemSchemaServerQuery(
        adminClient,
        SYS_SCHEMA_SERVERS_QUERY,
        getServersWithoutCurrentSize(adminServers)
    );

    LOG.info("Checking sys.server_segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments
    );

    LOG.info("Checking sys.tasks query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );
  }

  @Test
  public void test_unsecuredPathWithoutCredentials_allowed()
  {
    // check that we are allowed to access unsecured path without credentials.
    checkUnsecuredCoordinatorLoadQueuePath(httpClient);
  }

  @Test
  public void test_admin_hasNodeAccess()
  {
    checkNodeAccess(adminClient);
  }

  @Test
  public void test_admin_optionsRequest()
  {
    verifyAdminOptionsRequest();
  }

  @Test(expectedExceptions = { RuntimeException.class })
  public void test_nonExistent_fails()
  {
    druidUserNotExistClient = buildHttpClientForUser("doesNotExist", "bogus");
  }

  @Test
  public void test_role_lifecycle() throws Exception
  {
    // create a role that can only read 'auth_test'
    String roleName = "role_test_role_lifecycle";
    List<ResourceAction> resourceActions = Collections.singletonList(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        )
    );

    createRoleWithPermissions(ImmutableMap.of(roleName, resourceActions));

    // get roles and ensure that the role is there
    StatusResponseHolder responseHolder = makeGetRolesRequest(adminClient, HttpResponseStatus.OK);
    String content = responseHolder.getContent();
    Set<String> roles = jsonMapper.readValue(content, ROLES_RESULTS_TYPE_REFERENCE);
    Assert.assertTrue(roles.contains(roleName));

    // get role permissions and ensure that they match what is expected
    responseHolder = makeGetRoleRequest(adminClient, roleName, HttpResponseStatus.OK);
    content = responseHolder.getContent();
    KeycloakAuthorizerRoleSimplifiedPermissions roleWithPermissions = jsonMapper.readValue(
        content,
        KeycloakAuthorizerRoleSimplifiedPermissions.class
    );
    Assert.assertEquals(roleName, roleWithPermissions.getName());
    Assert.assertEquals(resourceActions, roleWithPermissions.getPermissions());

    // update role permissions and ensure that they match what is expected
    List<ResourceAction> updatedResourceActions = ImmutableList.of(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        ),
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.WRITE
        )
    );
    makeUpdateRoleRequest(adminClient, roleName, updatedResourceActions, HttpResponseStatus.OK);
    responseHolder = makeGetRoleRequest(adminClient, roleName, HttpResponseStatus.OK);
    content = responseHolder.getContent();
    roleWithPermissions = jsonMapper.readValue(
        content,
        KeycloakAuthorizerRoleSimplifiedPermissions.class
    );
    Assert.assertEquals(roleName, roleWithPermissions.getName());
    Assert.assertEquals(updatedResourceActions, roleWithPermissions.getPermissions());

    // get permissions for role separtely from role and ensure that they are what is expected
    List<KeycloakAuthorizerPermission> expectedPermissions = updatedResourceActions
        .stream()
        .map(r -> new KeycloakAuthorizerPermission(r, Pattern.compile(r.getResource().getName())))
        .collect(Collectors.toList());
    responseHolder = makeGetRolePermissionsRequest(adminClient, roleName, HttpResponseStatus.OK);
    content = responseHolder.getContent();
    List<KeycloakAuthorizerPermission> actualPermissions = jsonMapper.readValue(
        content,
        ROLE_PERMISSIONS_RESULT_TYPE_REFERENCE
    );
    Assert.assertEquals(expectedPermissions, actualPermissions);

    // delete role and ensure that it is no longer there
    makeDeleteRoleRequest(adminClient, roleName, HttpResponseStatus.OK);
    responseHolder = makeGetRolesRequest(adminClient, HttpResponseStatus.OK);
    content = responseHolder.getContent();
    roles = jsonMapper.readValue(content, ROLES_RESULTS_TYPE_REFERENCE);
    Assert.assertFalse(roles.contains(roleName));
  }

  @Override
  protected void setupUsers()
  {

  }

  @Override
  protected void setupTestSpecificHttpClients()
  {
    adminClient = buildHttpClientForUser("admin", "priest");
  }

  @Override
  protected String getAuthenticatorName()
  {
    return KEYCLOAK_AUTHENTICATOR;
  }

  @Override
  protected String getAuthorizerName()
  {
    return KEYCLOAK_AUTHORIZER;
  }

  @Override
  protected String getExpectedAvaticaAuthError()
  {
    return null;
  }

  private void createRoleWithPermissions(
      Map<String, List<ResourceAction>> roleTopermissions
  ) throws Exception
  {
    roleTopermissions.keySet().forEach(role -> HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/db/roles/%s",
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
              "%s/druid-ext/keycloak-security/authorization/db/roles/%s/permissions",
              config.getCoordinatorUrl(),
              role
          ),
          permissionsBytes
      );
    }
  }

  private KeycloakedHttpClient buildHttpClientForUser(
      String username,
      String password
  )
  {
    try {
      AdapterConfig userConfig = new AdapterConfig();
      userConfig.setAuthServerUrl("http://imply-keycloak:8080/auth");
      userConfig.setRealm("druid");
      userConfig.setResource("druid-user-client");
      userConfig.setBearerOnly(true);
      userConfig.setCredentials(ImmutableMap.of(
          "provider", "secret",
          "secret", "druid-user-secret"
      ));
      userConfig.setSslRequired("NONE");

      KeycloakDeployment userDeployment = KeycloakDeploymentBuilder.build(userConfig);
      Map<String, String> reqHeaders = new HashMap<>();
      Map<String, String> reqParams = new HashMap<>();
      reqParams.put(OAuth2Constants.GRANT_TYPE, OAuth2Constants.PASSWORD);
      reqParams.put(OAuth2Constants.USERNAME, username);
      reqParams.put(OAuth2Constants.PASSWORD, password);
      ClientCredentialsProviderUtils.setClientCredentials(userDeployment, reqHeaders, reqParams);
      return new KeycloakedHttpClient(userDeployment, httpClient, false, reqHeaders, reqParams);
    }
    catch (Exception e) {
      LOG.error("exception occured");
      throw e;
    }
  }

  StatusResponseHolder makeGetRolesRequest(
      HttpClient httpClient,
      HttpResponseStatus expectedStatus
  )
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.GET,
        StringUtils.format("%s/druid-ext/keycloak-security/authorization/db/roles", config.getCoordinatorUrl()),
        null,
        expectedStatus
    );
  }

  StatusResponseHolder makeGetRoleRequest(
      HttpClient httpClient,
      String role,
      HttpResponseStatus expectedStatus
  )
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.GET,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/db/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null,
        expectedStatus
    );
  }

  StatusResponseHolder makeGetRolePermissionsRequest(
      HttpClient httpClient,
      String role,
      HttpResponseStatus expectedStatus
  )
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.GET,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/db/roles/%s/permissions",
            config.getCoordinatorUrl(),
            role
        ),
        null,
        expectedStatus
    );
  }

  StatusResponseHolder makeUpdateRoleRequest(
      HttpClient httpClient,
      String role,
      List<ResourceAction> resourceActions,
      HttpResponseStatus expectedStatus
  ) throws JsonProcessingException
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/db/roles/%s/permissions",
            config.getCoordinatorUrl(),
            role
        ),
        jsonMapper.writeValueAsBytes(resourceActions),
        expectedStatus
    );
  }

  StatusResponseHolder makeDeleteRoleRequest(
      HttpClient httpClient,
      String role,
      HttpResponseStatus expectedStatus
  )
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.DELETE,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/db/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null,
        expectedStatus
    );
  }
}
