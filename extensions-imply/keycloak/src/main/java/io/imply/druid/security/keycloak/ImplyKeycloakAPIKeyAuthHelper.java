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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.druid.java.util.common.logger.Logger;
import org.keycloak.adapters.KeycloakDeployment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/**
 * Helper class that lets the caller exchange an API key for an auth token.
 *
 * Internally this class manages a local map of API keys to {@link TokenManager}s to leverage existing token refresh
 * logic.
 */
public class ImplyKeycloakAPIKeyAuthHelper
{
  private static final Logger LOG = new Logger(ImplyKeycloakAPIKeyAuthHelper.class);
  private static final String DEFAULT_APIKEY_CLIENT_ID = "api-key";
  private static final int EXPECTED_KEY_LENGTH = 70;
  private final String clientId;
  private final DruidKeycloakConfigResolver configResolver;
  private final Cache<String, TokenManager> tokenManagerCache = CacheBuilder
      .newBuilder()
      .initialCapacity(500)
      .build();

  public ImplyKeycloakAPIKeyAuthHelper(
      DruidKeycloakConfigResolver configResolver
  )
  {
    this(
        configResolver,
        DEFAULT_APIKEY_CLIENT_ID
    );
  }

  public ImplyKeycloakAPIKeyAuthHelper(
      DruidKeycloakConfigResolver configResolver,
      String clientId
  )
  {
    this.configResolver = Preconditions.checkNotNull(configResolver, "configResolver");
    this.clientId = clientId;
  }

  @VisibleForTesting
  static TokenManager newTokenManager(
      KeycloakDeployment deployment,
      String clientId,
      String apiKey
  )
  {
    HashMap<String, String> formParams = new HashMap<>();
    formParams.put("grant_type", "password");
    formParams.put("client_id", clientId);
    formParams.put("apiKey", apiKey);
    return new TokenManager(
        deployment,
        Collections.emptyMap(),
        formParams
    );
  }

  @Nullable
  public String getAccessTokenForAPIKey(String apiKey)
  {
    if (apiKey.length() != EXPECTED_KEY_LENGTH) {
      return null;
    }

    TokenManager tokenManager;
    try {
      tokenManager = tokenManagerCache.get(
          apiKey,
          () -> newTokenManager(
              configResolver.getUserDeployment(),
              clientId,
              apiKey
          )
      );
    }
    catch (ExecutionException e) {
      LOG.error(e, "Error constructing TokenManager for API key auth.");
      return null;
    }

    return tokenManager.getAccessTokenString();
  }
}
