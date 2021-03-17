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
