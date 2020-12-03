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
import org.keycloak.adapters.spi.HttpFacade.Request;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.mockito.Mockito;

public class DruidKeycloakConfigResolverTest
{
  @Test
  public void testResolve()
  {
    final AdapterConfig internalConfig = new AdapterConfig();
    internalConfig.setRealm("internal");
    internalConfig.setResource("internal");
    internalConfig.setAuthServerUrl("http://interal-auth");
    final AdapterConfig userConfig = new AdapterConfig();
    userConfig.setRealm("user");
    userConfig.setResource("user");
    userConfig.setAuthServerUrl("http://user-auth");
    final DruidKeycloakConfigResolver resolver = new DruidKeycloakConfigResolver(
        internalConfig,
        userConfig
    );
    final Request request = Mockito.mock(Request.class);
    Mockito.when(request.getHeader(DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER))
           .thenReturn(DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER_VALUE);
    Assert.assertEquals("internal", resolver.resolve(request).getRealm());

    Mockito.when(request.getHeader(DruidKeycloakConfigResolver.IMPLY_INTERNAL_REQUEST_HEADER))
           .thenReturn("Im not Druid");
    Assert.assertEquals("user", resolver.resolve(request).getRealm());
  }
}
