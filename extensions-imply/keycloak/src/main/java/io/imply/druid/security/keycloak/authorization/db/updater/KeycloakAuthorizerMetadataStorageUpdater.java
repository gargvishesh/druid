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
 * Implementations of this interface are responsible for connecting directly to the metadata storage,
 * modifying the authorizer database state or reading it. This interface is used by the
 * CoordinatorKeycloakAuthorizerResourceHandler (for handling configuration read/writes).
 */
public interface KeycloakAuthorizerMetadataStorageUpdater
{
  byte[] getCurrentRoleMapBytes();

  void createRole(String roleName);

  void deleteRole(String roleName);

  void setPermissions(String roleName, List<ResourceAction> permissions);
}
