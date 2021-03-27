/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Test;
import org.keycloak.representations.adapters.config.AdapterConfig;

public class ImplyKeycloakEscalatorTest
{
  private static final String AUTHORIZER_NAME = "keycloak-authorizer";

  @Test
  public void test_reateEscalatedAuthenticationResult_resultWithAdminRoleInContext()
  {
    final AdapterConfig internalConfig = new AdapterConfig();
    internalConfig.setRealm("internal");
    internalConfig.setResource("internal");
    internalConfig.setAuthServerUrl("http://interal-auth");
    ImplyKeycloakEscalator escalator = new ImplyKeycloakEscalator(AUTHORIZER_NAME, internalConfig);
    AuthenticationResult expectedResult = new AuthenticationResult(
        "internal",
        AUTHORIZER_NAME,
        null,
        KeycloakAuthUtils.CONTEXT_WITH_ADMIN_ROLE
    );
    AuthenticationResult actualResult = escalator.createEscalatedAuthenticationResult();
  }
}
