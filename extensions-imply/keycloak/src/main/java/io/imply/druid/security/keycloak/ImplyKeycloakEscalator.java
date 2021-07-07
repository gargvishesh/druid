/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.representations.adapters.config.AdapterConfig;

@JsonTypeName("imply-keycloak")
public class ImplyKeycloakEscalator implements Escalator
{
  private final String authorizerName;
  private final KeycloakDeployment keycloakDeployment;

  @JsonCreator
  public ImplyKeycloakEscalator(
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject @EscalatedGlobal AdapterConfig internalConfig
  )
  {
    this.authorizerName = authorizerName;
    this.keycloakDeployment = KeycloakDeploymentBuilder.build(internalConfig);
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return new KeycloakedHttpClient(keycloakDeployment, baseClient);
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return new AuthenticationResult(
        keycloakDeployment.getResourceName(),
        authorizerName,
        null,
        KeycloakAuthUtils.CONTEXT_WITH_ADMIN_ROLE
    );
  }

  public KeycloakDeployment getKeycloakDeployment()
  {
    return keycloakDeployment;
  }
}
