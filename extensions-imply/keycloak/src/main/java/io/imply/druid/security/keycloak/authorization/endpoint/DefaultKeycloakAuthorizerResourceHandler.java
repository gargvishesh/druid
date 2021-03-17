package io.imply.druid.security.keycloak.authorization.endpoint;

import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.List;

public class DefaultKeycloakAuthorizerResourceHandler implements KeycloakAuthorizerResourceHandler
{
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  @Override
  public Response getAllRoles()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteRole(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response setRolePermissions(String roleName, List<ResourceAction> permissions)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRolePermissions(String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }
}
