/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.security.keycloak.KeycloakSecurityResourceFilter;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.ResourceAction;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/druid-ext/keycloak-security/authorization")
@LazySingleton
public class KeycloakAuthorizerResource
{
  private final KeycloakAuthorizerResourceHandler resourceHandler;

  @Inject
  public KeycloakAuthorizerResource(
      KeycloakAuthorizerResourceHandler resourceHandler,
      AuthValidator authValidator
  )
  {
    this.resourceHandler = resourceHandler;
  }

  /**
   * @param req HTTP request
   *
   * @return List of all roles
   */
  @GET
  @Path("/roles")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response getAllRoles(
      @Context HttpServletRequest req
  )
  {
    return resourceHandler.getAllRoles();
  }

  /**
   * Get info about a role
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return Role name, and permissions of role. 400 error if role doesn't exist.
   */
  @GET
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response getRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") final String roleName
  )
  {
    return resourceHandler.getRole(roleName);
  }

  /**
   * Create a new role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role already exists
   */
  @POST
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response createRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") final String roleName
  )
  {
    return resourceHandler.createRole(roleName);
  }

  /**
   * Delete a role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role doesn't exist.
   */
  @DELETE
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response deleteRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName
  )
  {
    return resourceHandler.deleteRole(roleName);
  }

  /**
   * Set the permissions of a role. This replaces the previous permissions of the role.
   *
   * @param req         HTTP request
   * @param roleName    Name of role
   * @param permissions Permissions to set
   *
   * @return OK response. 400 error if role doesn't exist.
   */
  @POST
  @Path("/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response setRolePermissions(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName,
      List<ResourceAction> permissions
  )
  {
    return resourceHandler.setRolePermissions(roleName, permissions);
  }

  /**
   * Get the permissions of a role.
   *
   * @param req         HTTP request
   * @param roleName    Name of role
   *
   * @return OK response. 400 error if role doesn't exist.
   */
  @GET
  @Path("/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response getRolePermissions(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName
  )
  {
    return resourceHandler.getRolePermissions(roleName);
  }

  /**
   * Listen for update notifications for the role auth storage
   */
  @POST
  @Path("/listen/roles")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response authorizerRoleUpdateListener(
      @Context HttpServletRequest req,
      byte[] serializedRoleMap
  )
  {
    return resourceHandler.authorizerRoleUpdateListener(serializedRoleMap);
  }

  /**
   * Listen for update notifications for the role auth storage
   */
  @POST
  @Path("/listen/not-before")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response authorizerNotBeforeUpdateListener(
      @Context HttpServletRequest req,
      byte[] serializedNotBeforeMap
  )
  {
    return resourceHandler.authorizerNotBeforeUpdateListener(serializedNotBeforeMap);
  }

  /**
   * @param req HTTP request
   *
   * Sends an "update" notification to all services with the current role database state,
   * causing them to refresh their DB cache state.
   */
  @GET
  @Path("/refreshAll")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response refreshAll(
      @Context HttpServletRequest req
  )
  {
    return resourceHandler.refreshAll();
  }

  /**
   * @param req HTTP request
   *
   * @return serialized role map
   */
  @GET
  @Path("/cachedSerializedRoleMap")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response getCachedSerializedRoleMap(
      @Context HttpServletRequest req
  )
  {
    return resourceHandler.getCachedRoleMaps();
  }

  /**
   * @param req HTTP request
   *
   * @return serialized not before token revocation map
   */
  @GET
  @Path("/cachedSerializedNotBeforeMap")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(KeycloakSecurityResourceFilter.class)
  public Response getCachedSerializedNotBeforeMap(
      @Context HttpServletRequest req
  )
  {
    return resourceHandler.getCachedNotBeforeMaps();
  }
}
