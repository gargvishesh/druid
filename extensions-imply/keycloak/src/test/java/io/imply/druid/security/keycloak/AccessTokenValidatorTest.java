/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.common.VerificationException;
import org.keycloak.representations.AccessToken;
import org.mockito.Mockito;

import java.util.Set;

public class AccessTokenValidatorTest
{
  private static final String AUTHORIZER_NAME = "keycloak-authorizer";
  private static final String AUTHENTICATOR_NAME = "keycloak-authenticator";
  private static final String USER1 = "user1";
  private static final String CLIENT_ID = "clusterId";
  private static final String ESCALATED_CLIENT_ID = "clusterId-internal";

  private KeycloakDeployment deployment;
  private KeycloakDeployment escalatedDeployment;
  private DruidKeycloakConfigResolver configResolver;
  private AccessTokenValidator validator;

  @Before
  public void setup()
  {
    configResolver = Mockito.mock(DruidKeycloakConfigResolver.class);
    deployment = Mockito.mock(KeycloakDeployment.class);
    escalatedDeployment = Mockito.mock(KeycloakDeployment.class);
    Mockito.when(configResolver.getUserDeployment()).thenReturn(deployment);
    Mockito.when(configResolver.getInternalDeployment()).thenReturn(escalatedDeployment);
    Mockito.when(deployment.getResourceName()).thenReturn(CLIENT_ID);
    Mockito.when(deployment.getNotBefore()).thenReturn(1);
    Mockito.when(escalatedDeployment.getResourceName()).thenReturn(ESCALATED_CLIENT_ID);
    Mockito.when(escalatedDeployment.getNotBefore()).thenReturn(1);
    validator = Mockito.spy(new AccessTokenValidator(AUTHENTICATOR_NAME, AUTHORIZER_NAME, configResolver));
  }

  @Test
  public void test_authenticateToken_verificationException_returnsNull() throws VerificationException
  {
    Mockito.doThrow(new VerificationException("Access token failed verification"))
           .when(validator)
           .verifyToken("bogus", deployment);
    Assert.assertNull(validator.authenticateToken("bogus", deployment));
  }

  @Test
  public void test_authenticateToken_tokenIssueTimeBeforeNotBefore_returnsNull() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(0L);
    Mockito.when(deployment.getNotBefore()).thenReturn(1);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    Assert.assertNull(validator.authenticateToken("token", deployment));
  }

  @Test
  public void test_authenticateToken_tokenHasNullCustomClaims_returnsResultWithEmptyRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER1);
    Mockito.when(accessToken.getResourceAccess(CLIENT_ID)).thenReturn(null);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        USER1,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasNoResourceAccess_returnsResultWithEmptyRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER1);
    Mockito.when(accessToken.getResourceAccess(CLIENT_ID)).thenReturn(null);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        USER1,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasResourceAccessButNoRoles_returnsResultWithEmptyRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER1);
    Mockito.when(accessToken.getResourceAccess(CLIENT_ID)).thenReturn(new AccessToken.Access());
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        USER1,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasResourceAccessAndRoles_returnsResultWithRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER1);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    Mockito.when(accessToken.getResourceAccess(CLIENT_ID)).thenReturn(new AccessToken.Access().roles(roles));
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        USER1,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasResourceAccessAndRoles_returnsResultWithRolesSuperuser() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER1);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    Mockito.when(accessToken.getResourceAccess(CLIENT_ID)).thenReturn(new AccessToken.Access().roles(roles));
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", escalatedDeployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        USER1,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(
            KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles,
            KeycloakAuthUtils.SUPERUSER_CONTEXT_KEY, true
        )
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", escalatedDeployment);
    Assert.assertEquals(expectedResult, actualResult);
  }
}
