/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.common.VerificationException;
import org.keycloak.representations.AccessToken;
import org.mockito.Mockito;

import java.util.List;

public class AccessTokenValidatorTest
{
  private static final String AUTHORIZER_NAME = "keycloak-authorizer";
  private static final String ROLE_TOKEN_CLAIM = "imply-roles";

  private KeycloakDeployment deployment;

  private AccessTokenValidator validator;

  @Before
  public void setup()
  {
    deployment = Mockito.mock(KeycloakDeployment.class);
    validator = Mockito.spy(new AccessTokenValidator(AUTHORIZER_NAME, ROLE_TOKEN_CLAIM));
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
    Mockito.when(accessToken.getPreferredUsername()).thenReturn("user1");
    Mockito.when(accessToken.getOtherClaims()).thenReturn(null);
    Mockito.when(deployment.getNotBefore()).thenReturn(1);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        "user1",
        AUTHORIZER_NAME,
        null,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasNonListCustomRolClaim_returnsResultWithEmptyRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn("user1");
    Mockito.when(accessToken.getOtherClaims()).thenReturn(ImmutableMap.of(ROLE_TOKEN_CLAIM, "notList"));
    Mockito.when(deployment.getNotBefore()).thenReturn(1);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        "user1",
        AUTHORIZER_NAME,
        null,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }

  @Test
  public void test_authenticateToken_tokenHasListCustomRolClaim_returnsResultWithRoles() throws VerificationException
  {
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getIat()).thenReturn(2L);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn("user1");
    List<String> roles = ImmutableList.of("role1", "role2");
    Mockito.when(accessToken.getOtherClaims()).thenReturn(ImmutableMap.of(ROLE_TOKEN_CLAIM, roles));
    Mockito.when(deployment.getNotBefore()).thenReturn(1);
    Mockito.doReturn(accessToken).when(validator).verifyToken("token", deployment);
    AuthenticationResult expectedResult = new AuthenticationResult(
        "user1",
        AUTHORIZER_NAME,
        null,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles)
    );
    AuthenticationResult actualResult = validator.authenticateToken("token", deployment);
    Assert.assertEquals(expectedResult, actualResult);
  }
}
