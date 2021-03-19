/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.ArgumentMatchers.anyInt;

public class DruidKeycloakOIDCFilterTest
{
  private static final String AUTHENTICATOR_NAME = "keycloak-authenticator";
  private static final String AUTHORIZER_NAME = "keycloak-authorizer";
  private static final String ROLES_TOKEN_CLAIM = "druid-roles";
  private KeycloakConfigResolver configResolver;

  private DruidKeycloakOIDCFilter filter;

  @Before
  public void setup()
  {
    configResolver = Mockito.mock(KeycloakConfigResolver.class);
    filter = new DruidKeycloakOIDCFilter(configResolver, AUTHENTICATOR_NAME, AUTHORIZER_NAME, ROLES_TOKEN_CLAIM);
  }

  @Test
  public void testInit()
  {
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);

    final DruidKeycloakOIDCFilter filter = new DruidKeycloakOIDCFilter(
        Mockito.mock(KeycloakConfigResolver.class),
        "authenticator",
        "authorizer",
        ROLES_TOKEN_CLAIM
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
    Mockito.verify(res, Mockito.never()).sendError(anyInt());
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
    Mockito.verify(res, Mockito.never()).sendError(anyInt());
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
    Mockito.verify(res, Mockito.never()).sendError(anyInt());
  }
}
