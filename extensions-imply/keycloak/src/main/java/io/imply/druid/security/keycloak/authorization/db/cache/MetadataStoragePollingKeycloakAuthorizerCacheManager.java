/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;

import java.util.Map;

public class MetadataStoragePollingKeycloakAuthorizerCacheManager implements KeycloakAuthorizerCacheManager
{
  private final KeycloakAuthorizerMetadataStorageUpdater storageUpdater;

  @Inject
  public MetadataStoragePollingKeycloakAuthorizerCacheManager(
      Injector injector
  )
  {
    this.storageUpdater = injector.getInstance(KeycloakAuthorizerMetadataStorageUpdater.class);
  }

  @VisibleForTesting
  public MetadataStoragePollingKeycloakAuthorizerCacheManager()
  {
    this.storageUpdater = null;
  }

  @Override
  public void handleAuthorizerRoleUpdate(byte[] serializedRoleMap)
  {

  }

  @Override
  public Map<String, KeycloakAuthorizerRole> getRoleMap()
  {
    return storageUpdater.getCachedRoleMap();
  }
}
