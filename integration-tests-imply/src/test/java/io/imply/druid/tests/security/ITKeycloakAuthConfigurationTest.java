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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.imply.druid.security.keycloak.KeycloakedHttpClient;
import io.imply.druid.security.keycloak.TokenManager;
import io.imply.druid.security.keycloak.TokenService;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRoleSimplifiedPermissions;
import io.imply.druid.tests.ImplyTestNGGroup;
import org.apache.druid.java.util.common.DateTimes;
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
import org.keycloak.OAuth2Constants;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.authentication.ClientCredentialsProviderUtils;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.keycloak.representations.idm.ClientRepresentation;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: ForbiddenException: Authentication failed.";
  private static final String EXPECTED_AVATICA_AUTHZ_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: RuntimeException: org.apache.druid.server.security.ForbiddenException: Allowed:false, Message: -> ForbiddenException: Allowed:false, Message:";

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @BeforeClass
  public void before() throws Exception
  {
    // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
    );

    setupHttpClientsAndUsers();
    setExpectedSystemSchemaObjects();
    ITRetryUtil.retryUntilTrue(
        () -> getAllRoles().containsAll(
            ImmutableSet.of(
                "admin",
                "datasourceOnlyRole",
                "datasourceAndContextParamsRole",
                "datasourceWithSysRole",
                "datasourceWithStateRole",
                "stateOnlyRole"
            )
        ),
        "waiting all roles to be loaded"
    );
    // It is sad that we need a sleep here.
    // Because of the lack of an API to get the load status in the keycloak extension,
    // the check against `getAllRoles` above is not enough because `getAllRoles` calls the coordinator
    // while the config might have not been propagated to other servers yet.
    // To fix this, we should add a new API that returns the config load status.
    Thread.sleep(5000);
  }

  @Test
  public void test_systemSchemaAccess_admin_staleToken() throws Exception
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    // check that admin access works on all nodes
    checkNodeAccess(adminClient);
    int notBefore = Ints.checkedCast(DateTimes.nowUtc().plusHours(1).getMillis() / 1000);
    LOG.info("Setting keycloak not-before policy to %s", notBefore);
    setKeycloakClientNotBefore(notBefore);

    try {
      // sleep to let policy propagate
      Thread.sleep(1000 * 20);
      // as admin
      LOG.info("Checking sys.segments query as admin fails with token issued prior to not-before policy...");
      Map<String, Object> queryMap = ImmutableMap.of("query", SYS_SCHEMA_SERVERS_QUERY);
      StatusResponseHolder responseHolder = HttpUtil.makeRequestWithExpectedStatus(
          adminClient,
          HttpMethod.POST,
          this.config.getBrokerUrl() + "/druid/v2/sql",
          this.jsonMapper.writeValueAsBytes(queryMap),
          HttpResponseStatus.UNAUTHORIZED
      );
      Assert.assertEquals(HttpResponseStatus.UNAUTHORIZED, responseHolder.getStatus());
      Assert.assertTrue(responseHolder.getContent().contains("<tr><th>MESSAGE:</th><td>Unauthorized</td></tr>"));
    }
    finally {
      LOG.info("Resetting keycloak not-before policy to 0");
      setKeycloakClientNotBefore(0);
      // sleep to let policy propagate
      Thread.sleep(1000 * 30);
    }
  }

  @Test(expectedExceptions = {RuntimeException.class})
  public void test_nonExistent_fails()
  {
    setupHttpClientForUser("doesNotExist", "bogus");
  }

  @Test
  public void test_role_lifecycle() throws Exception
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    // create a role that can only read 'auth_test'
    String roleName = "role_test_role_lifecycle";
    List<ResourceAction> resourceActions = Collections.singletonList(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        )
    );

    createRolesWithPermissions(ImmutableMap.of(roleName, resourceActions));

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
  @Test
  public void test_admin_loadStatus()
  {
    // do nothing, this test is specific to the basic-security extension
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForUser(User user)
  {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty(
        "password",
        ((KeycloakedHttpClient) getHttpClient(user)).getAccessTokenString()
    );
    return connectionProperties;
  }

  @Override
  protected Properties getAvaticaConnectionPropertiesForInvalidAdmin()
  {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty(
        "Bearer",
        ((KeycloakedHttpClient) getHttpClient(User.ADMIN)).getAccessTokenString()
    );
    return connectionProperties;
  }

  @Override
  protected void setupDatasourceOnlyUser() throws Exception
  {
    createRolesWithPermissions(ImmutableMap.of("datasourceOnlyRole", DATASOURCE_ONLY_PERMISSIONS));
  }

  @Override
  protected void setupDatasourceAndContextParamsUser() throws Exception
  {
    createRolesWithPermissions(ImmutableMap.of("datasourceAndContextParamsRole", DATASOURCE_QUERY_CONTEXT_PERMISSIONS));
  }

  @Override
  protected void setupDatasourceAndSysTableUser() throws Exception
  {
    createRolesWithPermissions(ImmutableMap.of("datasourceWithSysRole", DATASOURCE_SYS_PERMISSIONS));
  }

  @Override
  protected void setupDatasourceAndSysAndStateUser() throws Exception
  {
    createRolesWithPermissions(ImmutableMap.of("datasourceWithStateRole", DATASOURCE_SYS_STATE_PERMISSIONS));
  }

  @Override
  protected void setupSysTableAndStateOnlyUser() throws Exception
  {
    createRolesWithPermissions(ImmutableMap.of("stateOnlyRole", STATE_ONLY_PERMISSIONS));
  }

  @Override
  protected void setupTestSpecificHttpClients()
  {
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
    return EXPECTED_AVATICA_AUTH_ERROR;
  }

  @Override
  protected String getExpectedAvaticaAuthzError()
  {
    return EXPECTED_AVATICA_AUTHZ_ERROR;
  }

  /**
   * Create roles with permissions in Druid.
   *
   * The roles should be also created in Keycloak to use them for Druid authorization.
   * See integration-tests-imply/docker/keycloak-configs/setup.sh for how to create a role in Keycloak.
   */
  private void createRolesWithPermissions(Map<String, List<ResourceAction>> roleToPermissions) throws Exception
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    roleToPermissions.keySet().forEach(role -> HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null
    ));

    for (Map.Entry<String, List<ResourceAction>> entry : roleToPermissions.entrySet()) {
      String role = entry.getKey();
      List<ResourceAction> permissions = entry.getValue();
      byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          StringUtils.format(
              "%s/druid-ext/keycloak-security/authorization/roles/%s/permissions",
              config.getCoordinatorUrl(),
              role
          ),
          permissionsBytes
      );
    }
  }

  private Set<String> getAllRoles() throws JsonProcessingException
  {
    final HttpClient adminClient = getHttpClient(User.ADMIN);
    StatusResponseHolder statusResponseHolder = HttpUtil.makeRequestWithExpectedStatus(
        adminClient,
        HttpMethod.GET,
        StringUtils.format(
            "%s/druid-ext/keycloak-security/authorization/roles",
            config.getCoordinatorUrl()
        ),
        null,
        HttpResponseStatus.OK
    );
    return jsonMapper.readValue(
        statusResponseHolder.getContent(),
        new TypeReference<Set<String>>()
        {
        }
    );
  }

  @Override
  protected HttpClient setupHttpClientForUser(
      String username,
      String password
  )
  {
    try {
      AdapterConfig userConfig = new AdapterConfig();
      userConfig.setAuthServerUrl("http://localhost:8080/auth");
      userConfig.setRealm("druid");
      userConfig.setResource("some-druid-cluster");
      userConfig.setBearerOnly(true);
      userConfig.setCredentials(ImmutableMap.of(
          "provider", "secret",
          "secret", "druid-user-secret"
      ));
      userConfig.setSslRequired("NONE");
      userConfig.setVerifyTokenAudience(false);

      KeycloakDeployment userDeployment = KeycloakDeploymentBuilder.build(userConfig);
      Map<String, String> reqHeaders = new HashMap<>();
      Map<String, String> reqParams = new HashMap<>();
      // Set Host header so that ISS references imply-keycloak, which is needed for verification on Druid side,
      // imply-keycloak is not routable from integration test code, and localhost:8080 wont work within docker
      // network where Druid Cluster is running.
      reqHeaders.put("Host", "imply-keycloak:8080");
      reqParams.put(OAuth2Constants.GRANT_TYPE, OAuth2Constants.PASSWORD);
      reqParams.put(OAuth2Constants.USERNAME, StringUtils.toLowerCase(username));
      reqParams.put(OAuth2Constants.PASSWORD, password);
      ClientCredentialsProviderUtils.setClientCredentials(userDeployment, reqHeaders, reqParams);
      TokenService tokenService = new TokenService(userDeployment, reqHeaders, reqParams);
      /* Disable token verification only on client side (within integration test), as it will fail if there is a
         discrepency between the Auth Server Url and the iss field found in the token. Token verification is only
        disabled on the Client side, not within Druid. */
      TokenManager tokenManager = new TokenManager(userDeployment, tokenService, false);
      return new KeycloakedHttpClient(httpClient, false, tokenManager);
    }
    catch (Exception e) {
      LOG.error("exception occured");
      throw e;
    }
  }

  void setKeycloakClientNotBefore(int notBefore)
  {
    Keycloak keycloak = KeycloakBuilder.builder()
                                       .serverUrl("http://localhost:8080/auth")
                                       .grantType(OAuth2Constants.PASSWORD)
                                       .realm("master")
                                       .clientId("admin-cli")
                                       .username("admin")
                                       .password("password")
                                       .build();

    keycloak.tokenManager().getAccessToken();
    RealmResource druidRealm = keycloak.realm("druid");
    List<ClientRepresentation> clientRepresentations = druidRealm.clients().findByClientId("some-druid-cluster");
    ClientRepresentation clientRepresentation = Iterables.getOnlyElement(clientRepresentations);
    clientRepresentation.setNotBefore(notBefore);
    druidRealm.clients().get(clientRepresentation.getId()).update(clientRepresentation);
  }

  StatusResponseHolder makeGetRolesRequest(
      HttpClient httpClient,
      HttpResponseStatus expectedStatus
  )
  {
    return HttpUtil.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.GET,
        StringUtils.format("%s/druid-ext/keycloak-security/authorization/roles", config.getCoordinatorUrl()),
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
            "%s/druid-ext/keycloak-security/authorization/roles/%s",
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
            "%s/druid-ext/keycloak-security/authorization/roles/%s/permissions",
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
            "%s/druid-ext/keycloak-security/authorization/roles/%s/permissions",
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
            "%s/druid-ext/keycloak-security/authorization/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null,
        expectedStatus
    );
  }
}
