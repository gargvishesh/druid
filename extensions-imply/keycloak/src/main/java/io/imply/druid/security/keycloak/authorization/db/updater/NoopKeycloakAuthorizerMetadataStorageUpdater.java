/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.updater;

import org.apache.druid.server.security.ResourceAction;

import java.util.List;

/**
 * Empty implementation of {@link KeycloakAuthorizerMetadataStorageUpdater}.
 * Void methods do nothing, other return empty maps or empty arrays depending on the return type.
 */
public class NoopKeycloakAuthorizerMetadataStorageUpdater implements KeycloakAuthorizerMetadataStorageUpdater
{
  @Override
  public byte[] getCurrentRoleMapBytes()
  {
    return new byte[0];
  }

  @Override
  public void createRole(String roleName)
  {
  }

  @Override
  public void deleteRole(String roleName)
  {
  }

  @Override
  public void setPermissions(String roleName, List<ResourceAction> permissions)
  {
  }
}
