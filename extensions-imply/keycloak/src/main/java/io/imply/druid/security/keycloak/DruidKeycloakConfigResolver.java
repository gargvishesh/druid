/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.inject.Inject;
import io.imply.druid.security.keycloak.authorization.state.cache.KeycloakAuthorizerCacheManager;
import org.apache.druid.server.security.Escalator;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.spi.HttpFacade.Request;
import org.keycloak.representations.adapters.config.AdapterConfig;

/**
 * This class is a sort of state-holder that is used to fetch {@link KeycloakDeployment} information for both the
 * external and internal (escalated) deployments. It also provides access to the {@link KeycloakAuthorizerCacheManager}
 * which provides both 'not-before' policy information used during authentication, and Keycloak role to permission
 * mapping which is used during authorization.
 *
 * This class also provides a proper configuration per-request basis, implementing {@link KeycloakConfigResolver}.
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
  private final KeycloakAuthorizerCacheManager cacheManager;

  @Inject
  public DruidKeycloakConfigResolver(
      Escalator escalator,
      AdapterConfig userConfig,
      KeycloakAuthorizerCacheManager cacheManager
  )
  {
    KeycloakDeployment internalDeployment = null;
    /* A different escalator type may be in use. Allow for the system to continue if no keycloak escalator is configured */
    if (escalator instanceof ImplyKeycloakEscalator) {
      internalDeployment = ((ImplyKeycloakEscalator) escalator).getKeycloakDeployment();
    }
    
    this.internalDeployment = internalDeployment;
    this.userDeployment = KeycloakDeploymentBuilder.build(userConfig);
    this.cacheManager = cacheManager;
  }

  @Override
  public KeycloakDeployment resolve(Request facade)
  {
    final String druidInternalHeader = facade.getHeader(IMPLY_INTERNAL_REQUEST_HEADER);
    return IMPLY_INTERNAL_REQUEST_HEADER_VALUE.equalsIgnoreCase(druidInternalHeader)
           ? internalDeployment
           : userDeployment;
  }

  public KeycloakDeployment getInternalDeployment()
  {
    return internalDeployment;
  }

  public KeycloakDeployment getUserDeployment()
  {
    return userDeployment;
  }

  public KeycloakAuthorizerCacheManager getCacheManager()
  {
    return cacheManager;
  }
}
