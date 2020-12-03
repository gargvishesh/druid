/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.spi.HttpFacade.Request;
import org.keycloak.representations.adapters.config.AdapterConfig;

/**
 * This class provides a proper configuration per-request basis.
 *
 * {@link DruidKeycloakOIDCFilter} performs authentication based on the {@link KeycloakDeployment}
 * resolved by this class. As a result, this class should be able to tell whether the given request
 * is issued by an internal service or something from outside. This is done by checking that
 * a custom HTTP header ({@link #IMPLY_INTERNAL_REQUEST_HEADER}) has the valid value.
 */
public class DruidKeycloakConfigResolver implements KeycloakConfigResolver
{
  static final String IMPLY_INTERNAL_REQUEST_HEADER = "X-IMPLY-INTERNAL-REQUEST";
  static final String IMPLY_INTERNAL_REQUEST_HEADER_VALUE = "IM-DRUID";

  private final KeycloakDeployment internalDeployment;
  private final KeycloakDeployment userDeployment;

  @Inject
  public DruidKeycloakConfigResolver(
      @EscalatedGlobal AdapterConfig internalConfig,
      AdapterConfig userConfig
  )
  {
    this.internalDeployment = KeycloakDeploymentBuilder.build(internalConfig);
    this.userDeployment = KeycloakDeploymentBuilder.build(userConfig);
  }

  @Override
  public KeycloakDeployment resolve(Request facade)
  {
    final String druidInternalHeader = facade.getHeader(IMPLY_INTERNAL_REQUEST_HEADER);
    return IMPLY_INTERNAL_REQUEST_HEADER_VALUE.equalsIgnoreCase(druidInternalHeader)
           ? internalDeployment
           : userDeployment;
  }

  @VisibleForTesting
  KeycloakDeployment getInternalDeployment()
  {
    return internalDeployment;
  }

  @VisibleForTesting
  KeycloakDeployment getUserDeployment()
  {
    return userDeployment;
  }
}
