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
import org.junit.Test;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;

public class DruidKeycloakOIDCFilterTest
{
  @Test
  public void testInit()
  {
    final ServletContext servletContext = Mockito.mock(ServletContext.class);
    final FilterConfig filterConfig = Mockito.mock(FilterConfig.class);
    Mockito.when(filterConfig.getServletContext()).thenReturn(servletContext);

    final DruidKeycloakOIDCFilter filter = new DruidKeycloakOIDCFilter(
        Mockito.mock(KeycloakConfigResolver.class),
        "authenticator",
        "authorizer"
    );
    filter.init(filterConfig);
    ArgumentCaptor<AdapterDeploymentContext> captor = ArgumentCaptor.forClass(AdapterDeploymentContext.class);
    Mockito.verify(servletContext)
           .setAttribute(ArgumentMatchers.eq(AdapterDeploymentContext.class.getName()), captor.capture());
    Assert.assertNotNull(captor.getValue());
  }
}
