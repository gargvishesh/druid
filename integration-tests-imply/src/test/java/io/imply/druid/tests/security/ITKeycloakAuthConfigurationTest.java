/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.security;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.KeycloakedHttpClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.security.AbstractAuthConfigurationTest;
import org.keycloak.OAuth2Constants;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.authentication.ClientCredentialsProviderUtils;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

// TODO: renable with IMPLY-6305
@Test(enabled = false)//, groups = ImplyTestNGGroup.KEYCLOAK_SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKeycloakAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITKeycloakAuthConfigurationTest.class);

  private static final String KEYCLOAK_AUTHENTICATOR = "ImplyKeycloakAuthenticator";
  private static final String KEYCLOAK_AUTHORIZER = "ImplyKeycloakAuthorizer";

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
    adminClient = buildHttpClientForUser("doesNotExist", "bogus");
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
      return new KeycloakedHttpClient(userDeployment, httpClient, reqHeaders, reqParams);
    }
    catch (Exception e) {
      LOG.error("exception occured");
      throw e;
    }
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
}
