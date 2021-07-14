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
import com.google.common.primitives.Ints;
import io.imply.druid.security.keycloak.authorization.state.cache.KeycloakAuthorizerCacheManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.KeycloakSecurityContext;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.AuthenticatedActionsHandler;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.OidcKeycloakAccount;
import org.keycloak.adapters.PreAuthActionsHandler;
import org.keycloak.adapters.servlet.FilterRequestAuthenticator;
import org.keycloak.adapters.servlet.OIDCFilterSessionStore;
import org.keycloak.adapters.servlet.OIDCServletHttpFacade;
import org.keycloak.adapters.spi.AuthOutcome;
import org.keycloak.adapters.spi.HttpFacade;
import org.keycloak.adapters.spi.KeycloakAccount;
import org.keycloak.representations.AccessToken;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DruidKeycloakOIDCFilterTest
{
  private static final String AUTHENTICATOR_NAME = "keycloak-authenticator";
  private static final String AUTHORIZER_NAME = "keycloak-authorizer";

  private static final String USER_IDENTIY = "user1";

  private static final String CLUSTER_CLIENT_ID = "clusterId";
  private static final String ISSUED_FOR = "external-client-id";
  private static final Map<String, Integer> IS_BEFORE = ImmutableMap.of(ISSUED_FOR, 0);
  private static final Map<String, Integer> NOT_BEFORE = ImmutableMap.of(
      ISSUED_FOR,
      Ints.checkedCast(DateTimes.nowUtc().plusHours(1).getMillis() / 1000)
  );

  private DruidKeycloakConfigResolver configResolver;
  private KeycloakAuthorizerCacheManager cacheManager;

  private DruidKeycloakOIDCFilter filter;

  @Before
  public void setup()
  {
    configResolver = Mockito.mock(DruidKeycloakConfigResolver.class);
    cacheManager = Mockito.mock(KeycloakAuthorizerCacheManager.class);
    filter = Mockito.spy(new DruidKeycloakOIDCFilter(
        configResolver,
        AUTHENTICATOR_NAME,
        AUTHORIZER_NAME
    ));
  }

  @Test
  public void testInit()
  {
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);

    final DruidKeycloakOIDCFilter filter = new DruidKeycloakOIDCFilter(
        Mockito.mock(DruidKeycloakConfigResolver.class),
        "authenticator",
        "authorizer"
    );
    filter.init(filterConfig);
    ArgumentCaptor<AdapterDeploymentContext> captor = ArgumentCaptor.forClass(AdapterDeploymentContext.class);
    Mockito.verify(servletContext)
           .setAttribute(ArgumentMatchers.eq(AdapterDeploymentContext.class.getName()), captor.capture());
    Assert.assertNotNull(captor.getValue());
  }

  @Test
  public void test_doFilter_requestWithNullAuthHeader_continuesToNextAuthenticator() throws Exception
  {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(null);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
  }

  @Test
  public void test_doFilter_requestWithAuthHeaderTooShort_continuesToNextAuthenticator() throws Exception
  {
    String header = "short";
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(header);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
  }

  @Test
  public void test_doFilter_requestWithAuthHeaderNotBearer_continuesToNextAuthenticator() throws Exception
  {
    String header = "SomeOtherAuthMethod";
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(header);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
  }

  @Test
  public void test_doFilter_resolvedDeploymentNull_returns403() throws IOException, ServletException
  {
    String header = "Bearer ";
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(header);
    Mockito.when(configResolver.resolve(ArgumentMatchers.any(HttpFacade.Request.class))).thenReturn(null);
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res).sendError(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  public void test_doFilter_resolvedDeploymentNotConfigured_returns403() throws IOException, ServletException
  {
    String header = "Bearer ";
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(header);
    KeycloakDeployment deployment = Mockito.mock(KeycloakDeployment.class);
    Mockito.when(deployment.isConfigured()).thenReturn(false);
    Mockito.when(configResolver.resolve(ArgumentMatchers.any(HttpFacade.Request.class))).thenReturn(deployment);
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res).sendError(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  public void test_doFilter_rolesInToken_butIssueBeforeCacheIsUnavailable_is401() throws IOException, ServletException
  {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        ISSUED_FOR,
        roles,
        null,
        false
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res).sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void test_doFilter_rolesInToken_butUnknownIssueFor_is401() throws IOException, ServletException
  {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        "unknown-client-id",
        roles,
        NOT_BEFORE,
        true
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res).sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void test_doFilter_rolesInToken_butIssuedBeforePolicy_is401() throws IOException, ServletException
  {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        ISSUED_FOR,
        roles,
        NOT_BEFORE,
        true
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    Mockito.verify(res).sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  public void test_doFilter_rolesInToken_createsAuthenticatedResultWithRoles() throws IOException, ServletException
  {

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    Set<String> roles = ImmutableSet.of("role1", "role2");
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        ISSUED_FOR,
        roles,
        IS_BEFORE,
        true
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    AuthenticationResult expectedAuthenticationResult = new AuthenticationResult(
        USER_IDENTIY,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, roles)
    );
    Mockito.verify(wrapper).setAttribute(ArgumentMatchers.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), ArgumentMatchers.eq(expectedAuthenticationResult));
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
    Mockito.verify(filterChain).doFilter(wrapper, res);
  }

  @Test
  public void test_doFilter_resourceNotInToken_createsAuthenticatedResultWithEmptyRoles() throws IOException, ServletException
  {

    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        ISSUED_FOR,
        null,
        IS_BEFORE,
        false
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    AuthenticationResult expectedAuthenticationResult = new AuthenticationResult(
        USER_IDENTIY,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    Mockito.verify(wrapper).setAttribute(ArgumentMatchers.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), ArgumentMatchers.eq(expectedAuthenticationResult));
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
    Mockito.verify(filterChain).doFilter(wrapper, res);
  }

  @Test
  public void test_doFilter_resourceInTokenButNoRoles_createsAuthenticatedResultWithEmptyRoles() throws IOException, ServletException
  {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse res = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    HttpServletRequestWrapper wrapper = Mockito.mock(HttpServletRequestWrapper.class);
    setupBearerAuthenticatedTestMocks(
        req,
        res,
        filterChain,
        filterConfig,
        wrapper,
        ISSUED_FOR,
        null,
        IS_BEFORE,
        true
    );
    filter.init(filterConfig);
    filter.doFilter(req, res, filterChain);
    AuthenticationResult expectedAuthenticationResult = new AuthenticationResult(
        USER_IDENTIY,
        AUTHORIZER_NAME,
        AUTHENTICATOR_NAME,
        ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, KeycloakAuthUtils.EMPTY_ROLES)
    );
    Mockito.verify(wrapper).setAttribute(ArgumentMatchers.eq(AuthConfig.DRUID_AUTHENTICATION_RESULT), ArgumentMatchers.eq(expectedAuthenticationResult));
    Mockito.verify(res, Mockito.never()).sendError(ArgumentMatchers.anyInt());
    Mockito.verify(filterChain).doFilter(wrapper, res);
  }

  private void setupBearerAuthenticatedTestMocks(
      HttpServletRequest req,
      HttpServletResponse res,
      FilterChain filterChain,
      FilterConfig filterConfig,
      HttpServletRequestWrapper wrapper,
      String issuedFor,
      Set<String> roles,
      Map<String, Integer> notBefore,
      boolean hasResourceAccess
  ) throws IOException, ServletException
  {
    String header = "Bearer ";
    Mockito.doNothing().when(filterChain).doFilter(req, res);
    Mockito.when(req.getHeader("Authorization")).thenReturn(header);
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getPreferredUsername()).thenReturn(USER_IDENTIY);
    Mockito.when(accessToken.getIssuedFor()).thenReturn(issuedFor);
    if (hasResourceAccess) {
      Mockito.when(accessToken.getResourceAccess(CLUSTER_CLIENT_ID)).thenReturn(new AccessToken.Access().roles(roles));
    } else {
      Mockito.when(accessToken.getResourceAccess(CLUSTER_CLIENT_ID)).thenReturn(null);
    }
    KeycloakSecurityContext securityContext = Mockito.mock(KeycloakSecurityContext.class);
    Mockito.when(securityContext.getToken()).thenReturn(accessToken);
    OidcKeycloakAccount account = Mockito.mock(OidcKeycloakAccount.class);
    Mockito.when(account.getKeycloakSecurityContext()).thenReturn(securityContext);
    Mockito.when(req.getSession()).thenReturn(null);
    Mockito.when(req.getAttribute(KeycloakAccount.class.getName())).thenReturn(account);
    KeycloakDeployment deployment = Mockito.mock(KeycloakDeployment.class);
    Mockito.when(deployment.isConfigured()).thenReturn(true);
    Mockito.when(deployment.getResourceName()).thenReturn(CLUSTER_CLIENT_ID);
    Mockito.when(configResolver.resolve(ArgumentMatchers.any(HttpFacade.Request.class))).thenReturn(deployment);
    Mockito.when(configResolver.getUserDeployment()).thenReturn(deployment);
    Mockito.when(configResolver.getCacheManager()).thenReturn(cacheManager);
    Mockito.when(cacheManager.validateNotBeforePolicies()).thenReturn(true);
    Mockito.when(cacheManager.getNotBefore()).thenReturn(notBefore);
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);
    PreAuthActionsHandler preAuthActionsHandler = Mockito.mock(PreAuthActionsHandler.class);
    Mockito.when(preAuthActionsHandler.handleRequest()).thenReturn(false);
    Mockito.doReturn(preAuthActionsHandler).when(filter).getPreAuthActionsHandler(ArgumentMatchers.any(OIDCServletHttpFacade.class));
    OIDCFilterSessionStore tokenStore = Mockito.mock(OIDCFilterSessionStore.class);
    Mockito.when(tokenStore.buildWrapper()).thenReturn(wrapper);
    Mockito.doReturn(tokenStore).when(filter).getOIDCFilterSessionStore(
        ArgumentMatchers.any(HttpServletRequest.class),
        ArgumentMatchers.any(OIDCServletHttpFacade.class),
        ArgumentMatchers.anyInt(),
        ArgumentMatchers.any(KeycloakDeployment.class));
    FilterRequestAuthenticator authenticator = Mockito.mock(FilterRequestAuthenticator.class);
    Mockito.when(authenticator.authenticate()).thenReturn(AuthOutcome.AUTHENTICATED);
    Mockito.doReturn(authenticator).when(filter).getFilterRequestAuthenticator(
        ArgumentMatchers.any(KeycloakDeployment.class),
        ArgumentMatchers.any(OIDCFilterSessionStore.class),
        ArgumentMatchers.any(OIDCServletHttpFacade.class),
        ArgumentMatchers.any(HttpServletRequest.class),
        ArgumentMatchers.anyInt());
    AuthenticatedActionsHandler actionsHandler = Mockito.mock(AuthenticatedActionsHandler.class);
    Mockito.when(actionsHandler.handledRequest()).thenReturn(false);
    Mockito.doReturn(actionsHandler).when(filter).getAuthenticatedActionsHandler(
        ArgumentMatchers.any(KeycloakDeployment.class),
        ArgumentMatchers.any(OIDCServletHttpFacade.class));
  }
}
