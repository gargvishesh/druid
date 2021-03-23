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
import org.apache.druid.java.util.common.logger.Logger;
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
  private static final Logger LOG = new Logger(DruidKeycloakConfigResolver.class);

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
    KeycloakDeployment internalDeployment = null;
    try {
      internalDeployment = KeycloakDeploymentBuilder.build(internalConfig);
    }
    catch (Exception e) {
      LOG.warn(
          e,
          "Failed to build internal keycloak escalator config. Cluster internal communication cannot be authenticated or authorized using keycloak"
      );
    }
    this.internalDeployment = internalDeployment;
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
